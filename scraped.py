import math
from pathlib2 import Path
from typing import List, Optional, Type

from igc_lib.igc_lib import Flight, FlightParsingConfig, GNSSFix

class ScrapedFlight(Flight):
    @staticmethod
    def create_from_file(filename: str, config_class: Type[FlightParsingConfig] = FlightParsingConfig) -> 'ScrapedFlight':
        config: FlightParsingConfig = config_class()
        fixes: List[GNSSFix] = []
        a_records: List[str] = []
        i_records: List[str] = []
        h_records: List[str] = []
        l_records: List[str] = []
        abs_filename: Path = Path(filename).expanduser().absolute()
        with abs_filename.open("r", encoding="ISO-8859-1") as flight_file:
            for line in flight_file:
                line = line.replace("\n", "").replace("\r", "")
                if not line:
                    continue
                if line[0] == "A":
                    a_records.append(line)
                elif line[0] == "B":
                    fix: Optional[GNSSFix] = GNSSFix.build_from_B_record(line, index=len(fixes))
                    if fix is not None:
                        if fixes and math.fabs(fix.rawtime - fixes[-1].rawtime) < 1e-5:
                            # The time did not change since the previous fix.
                            # Ignore this fix.
                            pass
                        else:
                            fixes.append(fix)
                elif line[0] == "I":
                    i_records.append(line)
                elif line[0] == "H":
                    h_records.append(line)
                elif line[0] == "L":
                    l_records.append(line)
                else:
                    # Do not parse any other types of IGC records
                    pass
        flight: ScrapedFlight = ScrapedFlight(
            fixes, a_records, h_records, l_records, i_records, config
        )
        return flight

    def __init__(self, fixes: List[GNSSFix], a_records: List[str], h_records: List[str], l_records: List[str], i_records: List[str], config: FlightParsingConfig) -> None:
        super().__init__(fixes, a_records, h_records, i_records, config)
        if l_records:
            self._parse_l_records(l_records)

    def _parse_l_records(self, l_records: List[str]) -> None:
        for record in l_records:
            self._parse_l_record(record)

    def _parse_l_record(self, record: str) -> None:
        if record[0:6] == "LSCR::":
            self._parse_lscr_record(record)

    def _parse_lscr_record(self, record: str) -> None:
        if record.startswith("LSCR::START:"):
            self.start = record.split("::", 1)[1].split(":", 1)[1].strip()
        elif record.startswith("LSCR::FINISH:"):
            self.finish = record.split("::", 1)[1].split(":", 1)[1].strip()
        elif record.startswith("LSCR::POINTS:"):
            self.points = int(record.split("::", 1)[1].split(":", 1)[1].strip())
        elif record.startswith("LSCR::CONTESTANT:"):
            self.pilot = record.split("::", 1)[1].split(":", 1)[1].strip()
        elif record.startswith("LSCR::COMPETITION:"):
            self.competition = record.split("::", 1)[1].split(":", 1)[1].strip()
        elif record.startswith("LSCR::CLASS:"):
            self.competition_class = record.split("::", 1)[1].split(":", 1)[1].strip()
