import requests
from bs4 import BeautifulSoup

def _get_header(host: str, referer: str = None):
    header = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
        "Host": host
    }

    if referer is not None:
        header["Referer"] = referer

    return header

def get_product_codes(keyword: str) -> list:
    response = requests.get("https://search.danawa.com/dsearch.php?query={}&tab=main".format(keyword),
                            headers=_get_header(host="search.danawa.com"))

    if response.status_code != 200:
        response.raise_for_status()

    bs = BeautifulSoup(response.text, "html.parser")
    prodlist = bs.find("div", {"class": "main_prodlist main_prodlist_list"})
    product_list = (prodlist or None) and prodlist.select_one("ul.product_list")
    products = (product_list or None) and product_list.find_all("li", {"class": "prod_item width_change"})

    if products is None:
        raise TypeError("can't find products.")

    product_codes = []

    for product in products:
        # get product code
        code1: str = product.get("id")
        code2 = (code1 or None) and code1.replace("productItem", "")
        code3 = -1

        if code2 is not None:
            code3 = int(code2)
        else:
            raise TypeError("can't find product code.")

        # get price
        price1 = product.find_next("input", {"id": "min_price_{}".format(code3)})
        price2 = (price1 or None) and price1.get("value")

        # get title
        title = product.select_one("div.prod_main_info > div.prod_info > p.prod_name > a")

        prod = {
            "code": code3
        }

        if price2 is not None:
            price3 = int(price2)
            prod["price"] = price3

        if title is not None:
            prod["title"] = title.text

        product_codes.append(prod)

    return product_codes

def get_product(product_code: int) -> dict:
    response = requests.get("https://prod.danawa.com/info/?pcode={}".format(product_code),
                            headers=_get_header(host="prod.danawa.com"))

    if response.status_code != 200:
        response.raise_for_status()

    bs = BeautifulSoup(response.text, "html.parser")
    summary_info = bs.select_one("div.summary_info")
    top_summary = (summary_info or None) and summary_info.select_one("div.top_summary")
    detail_summary = (summary_info or None) and summary_info.select_one("div.detail_summary")

    summary = {}

    if top_summary is not None:
        title = top_summary.select_one("div.top_summary > h3.prod_tit > span.title")
        spec = top_summary.select_one(
            "div.h_area > div.sub_dsc > div.spec_set_wrap > dl.spec_set > dd > div.spec_list > div.items")

        if title is not None:
            summary["title"] = title.text

        if spec is not None:
            summary["spec"] = spec.text

    if detail_summary is not None:
        image_link1 = detail_summary.select_one("div.summary_left > div.thumb_area > div.photo_w")
        image_link2 = (image_link1 or None) and image_link1.find("a", {"id": "imgExtensionAnchorLayer"})
        image_link3 = (image_link2 or None) and image_link2.select_one("img")

        lowest_price1 = detail_summary.select_one("div.summary_left > div.lowest_area > div.lowest_top")
        lowest_price2 = (lowest_price1 or None) and lowest_price1.find("div", {"class": "row lowest_price"})
        lowest_price3 = (lowest_price2 or None) and lowest_price2.select_one("span.lwst_prc > a > em.prc_c")

        prices = detail_summary.select("div.summary_left > div.lowest_area > div.lowest_list > table.lwst_tbl > tbody.high_list > tr")

        if image_link3 is not None:
            summary["img"] = image_link3.get("src")
        else:
            print(image_link1)

        if lowest_price3 is not None:
            summary["lowest_price"] = lowest_price3.text

        if prices is not None:
            summary["prices"] = []

            if lowest_price3 is not None:
                summary["prices"].append(summary["lowest_price"])

            for price in prices:
                class_name = price.get("class")
                price2 = price.select_one("td.price > a > span.txt_prc > em.prc_t")

                if len(class_name) == 0 and price2 is not None:
                    summary["prices"].append(price2.text)

    return summary

def get_price_variance(product_code: int, by_month=1) -> dict:
    if by_month != 1 and by_month != 3 and by_month != 6 and by_month != 12 and by_month != 24:
        raise AttributeError("by_month value must be 1, 3, 6, 12 or 24.")

    response = requests.get("https://prod.danawa.com/info/ajax/getProductPriceList.ajax.php?productCode={}&period=1&_=1666395869519".format(product_code),
                            headers=_get_header(host="prod.danawa.com", referer="danawa.com"))

    if response.status_code != 200:
        response.raise_for_status()

    json = response.json()
    json2 = json[str(by_month)]

    variance = {}

    if json2 is not None:
        variance["min"] = json2["minPrice"]
        variance["max"] = json2["maxPrice"]
        variance["prices"] = []

        for data in json2["result"]:
            price_data = {"price": data["minPrice"], "date": data["date"]}

            if "Fulldate" in data:
                price_data["full_date"] = data["Fulldate"]

            variance["prices"].append(price_data)
    else:
        raise TypeError("json is None")

    return variance