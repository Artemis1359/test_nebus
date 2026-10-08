"""Общие ошибки для сервисов и обработчиков сообщений и HTTP-запросов."""


class InvalidEvent(Exception):
    pass


class IdempotencyConflict(Exception):
    pass


class PaymentNotFound(Exception):
    pass


class GatewayError(Exception):
    """Эмуляция шлюза не обработала платёж; внешнее списание не выполнялось."""
