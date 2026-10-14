"""Names of RabbitMQ objects shared by the producer (outbox) and the consumer."""

PAYMENTS_EXCHANGE = "payments"
PAYMENTS_NEW = "payments.new"

PAYMENT_CREATED_EVENT = "payment.created"
