from fastscanner.services.indicators.ports import FundamentalData


class EODHDFundamentalStore:
    def get(self, symbol: str) -> "FundamentalData": ...
