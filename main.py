import pandas as pd
from utils.scraper import ImmoWebScraper
import time

_HOUSES = "house/for-sale?countries=BE&page={}"
_APARTMENTS = "apartment/for-sale?countries=BE&page={}"

if __name__ == "__main__":
    start = time.perf_counter()

    immoWebHouses = ImmoWebScraper(_HOUSES, 2)
    immoWebApartments = ImmoWebScraper(_APARTMENTS, 2)
    immoWebHouses.get_urls()
    immoWebApartments.get_urls()
    print(f'Recorded {len(immoWebHouses.data_list)} urls in {time.perf_counter() - start:.4f} seconds')
    
    immoWebHouses.scrap_data()
    immoWebApartments.scrap_data()
    print(f'Scrapped all data in {time.perf_counter() - start:.4f} seconds')
    
    df1 = immoWebHouses.fill_dataframe()
    df2 = immoWebApartments.fill_dataframe()

    df = pd.concat([df1, df2])
    #df.to_csv(r'./houses_to_sale.csv')

    print(f'Filled & saved dataframe of {len(immoWebHouses.data_list)} items in {time.perf_counter() - start:.4f} seconds')
