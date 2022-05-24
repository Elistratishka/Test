import os

from dotenv import load_dotenv
import urllib.request
import xml.dom.minidom as minidom


load_dotenv()


def get_data(xml_url):
    try:
        web_file = urllib.request.urlopen(xml_url)
        return web_file.read()
    except:
        pass


def get_currencies_dictionary(xml_content):

    dom = minidom.parseString(xml_content)
    dom.normalize()

    elements = dom.getElementsByTagName("Valute")
    currency_dict = {}

    for node in elements:
        for child in node.childNodes:
            if child.nodeType == 1:
                if child.tagName == 'Value':
                    if child.firstChild.nodeType == 3:
                        value = float(child.firstChild.data.replace(',', '.'))
                if child.tagName == 'CharCode':
                    if child.firstChild.nodeType == 3:
                        char_code = child.firstChild.data
        currency_dict[char_code] = value
    return currency_dict


def get_usd_rate():
    return get_currencies_dictionary(get_data(os.getenv("URL")))['USD']
