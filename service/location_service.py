from .database_service import Database


class LocationService(Database):
    def __init__(self):
        super().__init__()

    def get_all_locations(self):
        locations = self.db.location.find()
        return locations
