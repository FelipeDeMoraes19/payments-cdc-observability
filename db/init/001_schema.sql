CREATE TABLE customers (
    customer_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    full_name   text          NOT NULL,
    email       text          NOT NULL,
    cpf         char(11)      NOT NULL,
    created_at  timestamptz   NOT NULL DEFAULT now(),
    updated_at  timestamptz   NOT NULL DEFAULT now()
);

CREATE TABLE merchants (
    merchant_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    legal_name  text          NOT NULL,
    category    text          NOT NULL,
    country     char(2)       NOT NULL,
    created_at  timestamptz   NOT NULL DEFAULT now(),
    updated_at  timestamptz   NOT NULL DEFAULT now()
);

CREATE TABLE payments (
    payment_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id bigint        NOT NULL REFERENCES customers (customer_id),
    merchant_id bigint        NOT NULL REFERENCES merchants (merchant_id),
    amount      numeric(14,2) NOT NULL,
    currency    char(3)       NOT NULL,
    status      text          NOT NULL,
    created_at  timestamptz   NOT NULL DEFAULT now(),
    updated_at  timestamptz   NOT NULL DEFAULT now()
);

CREATE INDEX payments_customer_id_idx ON payments (customer_id);
CREATE INDEX payments_merchant_id_idx ON payments (merchant_id);
