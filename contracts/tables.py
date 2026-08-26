from dataclasses import dataclass
from decimal import Decimal
from typing import Optional, Tuple, Type

from pydantic import BaseModel, ConfigDict

from contracts.fields import PostgresTimestamp
from contracts.postgres_types import oid_for


class CustomerRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_id: int
    full_name: str
    email: str
    cpf: str
    created_at: PostgresTimestamp
    updated_at: PostgresTimestamp


class MerchantRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    merchant_id: int
    legal_name: str
    category: str
    country: str
    created_at: PostgresTimestamp
    updated_at: PostgresTimestamp


class PaymentRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payment_id: int
    customer_id: int
    merchant_id: int
    amount: Decimal
    currency: str
    status: str
    created_at: PostgresTimestamp
    updated_at: PostgresTimestamp


@dataclass(frozen=True)
class ColumnContract:
    name: str
    type_name: str
    is_key: bool = False

    @property
    def type_oid(self) -> int:
        return oid_for(self.type_name)


@dataclass(frozen=True)
class TableContract:
    namespace: str
    name: str
    columns: Tuple[ColumnContract, ...]
    model: Type[BaseModel]

    @property
    def qualified_name(self) -> str:
        return "{}.{}".format(self.namespace, self.name)

    @property
    def key_columns(self) -> Tuple[str, ...]:
        return tuple(column.name for column in self.columns if column.is_key)

    def column(self, name: str) -> Optional[ColumnContract]:
        for column in self.columns:
            if column.name == name:
                return column
        return None


CUSTOMERS = TableContract(
    namespace="public",
    name="customers",
    columns=(
        ColumnContract("customer_id", "int8", is_key=True),
        ColumnContract("full_name", "text"),
        ColumnContract("email", "text"),
        ColumnContract("cpf", "bpchar"),
        ColumnContract("created_at", "timestamptz"),
        ColumnContract("updated_at", "timestamptz"),
    ),
    model=CustomerRow,
)

MERCHANTS = TableContract(
    namespace="public",
    name="merchants",
    columns=(
        ColumnContract("merchant_id", "int8", is_key=True),
        ColumnContract("legal_name", "text"),
        ColumnContract("category", "text"),
        ColumnContract("country", "bpchar"),
        ColumnContract("created_at", "timestamptz"),
        ColumnContract("updated_at", "timestamptz"),
    ),
    model=MerchantRow,
)

PAYMENTS = TableContract(
    namespace="public",
    name="payments",
    columns=(
        ColumnContract("payment_id", "int8", is_key=True),
        ColumnContract("customer_id", "int8"),
        ColumnContract("merchant_id", "int8"),
        ColumnContract("amount", "numeric"),
        ColumnContract("currency", "bpchar"),
        ColumnContract("status", "text"),
        ColumnContract("created_at", "timestamptz"),
        ColumnContract("updated_at", "timestamptz"),
    ),
    model=PaymentRow,
)

CONTRACTS = {
    contract.qualified_name: contract for contract in (CUSTOMERS, MERCHANTS, PAYMENTS)
}


def contract_for(namespace: str, name: str) -> Optional[TableContract]:
    return CONTRACTS.get("{}.{}".format(namespace, name))
