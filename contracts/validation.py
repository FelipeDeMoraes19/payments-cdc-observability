from functools import lru_cache
from typing import Annotated

from pydantic import TypeAdapter, ValidationError

from contracts.postgres_types import describe
from contracts.tables import TableContract


class ContractViolation(Exception):
    pass


def _annotation_of(field):
    if not field.metadata:
        return field.annotation
    return Annotated[tuple([field.annotation, *field.metadata])]


@lru_cache(maxsize=None)
def _field_adapters(contract: TableContract) -> dict:
    return {
        name: TypeAdapter(_annotation_of(field))
        for name, field in contract.model.model_fields.items()
    }


def _at(lsn: str) -> str:
    return " at LSN {}".format(lsn) if lsn else ""


def validate_relation(relation, contract, lsn=None) -> None:
    if contract is None:
        raise ContractViolation(
            "relation {} is published but has no contract{}; either add one under "
            "contracts/ or remove the table from the publication".format(
                relation.qualified_name, _at(lsn)
            )
        )
    observed = {column.name: column for column in relation.columns}
    expected = {column.name: column for column in contract.columns}
    missing = sorted(set(expected) - set(observed))
    unexpected = sorted(set(observed) - set(expected))
    if missing or unexpected:
        raise ContractViolation(
            "relation {} does not match its contract{}: columns missing from the "
            "stream {}, columns absent from the contract {}".format(
                contract.qualified_name, _at(lsn), missing, unexpected
            )
        )
    for column in contract.columns:
        found = observed[column.name]
        if found.type_oid != column.type_oid:
            raise ContractViolation(
                "column {}.{} changed type{}: the contract expects {}, the stream "
                "carries {}".format(
                    contract.qualified_name,
                    column.name,
                    _at(lsn),
                    describe(column.type_oid),
                    describe(found.type_oid),
                )
            )
        if found.is_key != column.is_key:
            raise ContractViolation(
                "column {}.{} changed key membership{}: the contract says is_key={}, "
                "the stream says is_key={}".format(
                    contract.qualified_name,
                    column.name,
                    _at(lsn),
                    column.is_key,
                    found.is_key,
                )
            )


def validate_change(change, contract, lsn=None) -> None:
    values = change.old_values if change.action == "delete" else change.new_values
    if values is None:
        raise ContractViolation(
            "{} on {}{} carries no tuple to validate".format(
                change.action, change.relation.qualified_name, _at(lsn)
            )
        )
    if change.action == "insert":
        try:
            contract.model.model_validate(values)
        except ValidationError as error:
            raise ContractViolation(
                "insert on {}{} does not satisfy the contract: {}".format(
                    contract.qualified_name, _at(lsn), _first_problem(error)
                )
            ) from error
        return
    checkable = (
        {name: values[name] for name in contract.key_columns if name in values}
        if change.action == "delete"
        else values
    )
    adapters = _field_adapters(contract)
    for name, raw in checkable.items():
        try:
            adapters[name].validate_python(raw)
        except ValidationError as error:
            raise ContractViolation(
                "{} on {}.{}{} does not satisfy the contract: {}".format(
                    change.action,
                    contract.qualified_name,
                    name,
                    _at(lsn),
                    _first_problem(error),
                )
            ) from error


def _first_problem(error: ValidationError) -> str:
    problem = error.errors()[0]
    location = ".".join(str(part) for part in problem["loc"]) or "row"
    return "{}: {}".format(location, problem["msg"])
