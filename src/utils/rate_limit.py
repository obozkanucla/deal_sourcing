class BudgetExhausted(Exception):
    pass


class DailyClickBudget:
    def __init__(self, limit: int):
        self.limit = limit
        self.remaining = limit

    def consume(self):
        if self.remaining <= 0:
            raise BudgetExhausted("Daily detail-page budget exhausted")
        self.remaining -= 1