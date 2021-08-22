import urllib.request
import bs4

page = 'enter your url here'

def get_html(webpage):
    # Get the html from the pages in the list
    test_url = urllib.request.urlopen(webpage, timeout=10)
    # Create a beautiful soup objetc
    test_url = bs4.BeautifulSoup(test_url, features="lxml")
    # Find the relevent div in the html
    div_content = str(test_url.find(name='div', attrs={'Type of element (e.g: class)':'Name of class'}))
    return div_content

get_html(page)