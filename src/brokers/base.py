from abc import ABC, abstractmethod


class BrokerClient(ABC):

    @abstractmethod
    def login(self):
        pass

    @abstractmethod
    def fetch_index_listings(self):
        """
        Returns list of dicts:
        {
          source_listing_id,
          source_url,
          title
        }
        """
        pass

    @abstractmethod
    def fetch_listing_detail(self, listing):
        """
        Returns raw HTML for a listing detail page.
        """
        pass