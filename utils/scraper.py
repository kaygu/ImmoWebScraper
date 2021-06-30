from bs4 import BeautifulSoup
import json
import re
from typing import List, Union
import pandas as pd
from pandas.core.frame import DataFrame
import requests
from selenium import webdriver
from selenium.webdriver.firefox.options import Options


class Data:
    """
    This class stores all the data scraped from the websites and parses it.
    """
    def __init__(self, url: str):
        self.url: str = url

    def parse(self, data: dict) -> None:
        """
        Parses a dict and adds all data as class attributes

        :param data: Dictionarry containing data, containing info about house price, locations etc
        """
        transaction = data.get("transaction")
        property_ = data.get("property")
        self.id = data.get("id")
        self.transaction_type = transaction.get("type")
        self.price = transaction.get("sale").get("price")
        self.type = property_.get("type")
        self.subtype = property_.get("subtype")
        self.bedroom_count = property_.get("bedroomCount")
        self.bathroom_count = property_.get("bathroomCount")
        self.showerroom_count = property_.get("showerRoomCount")
        location = property_.get("location")
        if location:
            self.region = location.get("region")
            self.province = location.get("province")
            self.district = location.get("district")
            self.locality = location.get("locality")
            self.postal_code = location.get("postalCode")
            self.street_name = location.get("street")
            self.street_number = location.get("number")
        building = property_.get("building")
        if building:
            self.facades = building.get("facadeCount")
            self.condition = building.get("condition")
            self.construction_year = building.get("constructionYear")
        self.living_surface = property_.get("netHabitableSurface")
        self.garden_surface = property_.get("gardenSurface")
        self.terrace_surface = property_.get("terraceSurface")
        self.attic = property_.get("hasAttic")
        self.basement = property_.get("hasBasement")
        self.swimming_pool = property_.get("hasSwimmingPool")
        self.fireplace = property_.get("fireplaceExists")
        self.fitness_room = property_.get("hasFitnessRoom")
        self.tennis_court = property_.get("hasTennisCourt")
        self.sauna = property_.get("hasSauna")
        self.jacuzzi = property_.get("hasJacuzzi")
        self.hammam = property_.get("hasHammam")

class ImmoWebScraper:
    def __init__(self, query: str, pages: int = 333):
        """
        Scrap all the building data present on ImmoWeb.be
        :param link: Search link to reference every found house for sale. Must contain epty bracket for page number
        :param pages: Number of search pages to scrap trough
        """
        self.driver_options = Options()
        self.driver_options.headless = True
        self.driver: webdriver.Firefox = webdriver.Firefox(executable_path='../../geckodriver/geckodriver.exe', options=self.driver_options)
        self.driver.implicitly_wait(2)
        self._URL = f'https://www.immoweb.be/en/search/{query}'
        self.nb_pages = pages

        self.data_list: List[Union[Data, None]] = []

    def __del__(self):
        self.driver.quit()

    def get_urls(self) -> None:
        """
        Cycle through all the search pages of immoweb, saving all the announcements found in a Data structure
        """
        # Cycle through every page in the search engine
        for i in range(1, self.nb_pages +1):
            try:
                self.driver.get(self._URL.format(i))
                # Search for every announcement links (30 par search page)
                for elem in self.driver.find_elements_by_xpath("//a[@class='card__title-link']"):
                    link = elem.get_attribute("href")
                    if link:
                        self.data_list.append(Data(link))
                    else:
                        raise Exception("Link could not be found", link)
            except Exception as ex:
                self.data_list[i] = None
                print(ex)
            except:
                print("Someting went wrong in url parsing")
                raise

    def scrap_data(self) -> None:
        """
        Executes js script in the web page (returning window.dataLayer object), then adds it to dataFrame
        Maybe will need some webscraping with bs4, if data is incomplete

        For performance, maybe rewrite with http requests, avoids rendering pages ten thousand times 
        """

        for i, data in enumerate(self.data_list):
            try:
                req = requests.get(data.url)
                if req.status_code == 429:
                    raise Exception("Too many requests")
                if req.status_code != 200:
                    raise Exception(f"Status code : {req.status_code}")
                soup = BeautifulSoup(req.content, "lxml")
                # Searches for the first script tag, in our case the dataLayer we need is the first script
                start_pattern = re.compile(r'^\s+window.classified\s=\s')
                end_pattern = re.compile(r';\s+$')
                data_layer = soup.find("script", text=start_pattern).string

                # Remove js components from script

                if data_layer:  
                    raw = re.sub(start_pattern, '', data_layer.string)
                    raw = re.sub(end_pattern, '', raw)
                    raw = json.loads(raw)
                    # Removes house groupes & appartement groups
                    property_type = raw.get("property").get("type")
                    if property_type != "HOUSE_GROUP" and property_type != "APARTMENT_GROUP":
                        data.parse(raw)
                    else:
                        self.data_list[i] = None
                else:
                    raise Exception("DataLayer is undefined")
            except Exception as ex:
                self.data_list[i] = None
                print(ex)
            except:
                self.data_list[i] = None
                print("Someting bad happend")
                raise


    def fill_dataframe(self) -> pd.DataFrame:
        """
        Converts all the objects present in self.data_list to dictionaries, then feed them to the dataframe.
        """
        self.data_list = [i for i in self.data_list if i != None]
        self.df: DataFrame = pd.DataFrame([data.__dict__ for data in self.data_list])
        self.df.replace({'': None, 'None': None})
        return self.df
