CREATE TABLE public.items (
    item text,
    category text,
    vendor text,
    sale_price double precision,
    stock_status text,
    shipping_cost double precision,
    customer_review_score double precision,
    number_of_feedbacks bigint
);

ALTER TABLE public.items OWNER TO postgres;