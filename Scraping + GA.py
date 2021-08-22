import json
import urllib.request
import cachetools
import dill as pickle
import pandas as pd
from bs4 import BeautifulSoup as soup

# The code works in the following manner:
# Download CSV from GA -> Clean up data/Aggregate multiple CSV sheets -> Scrape the webpage -> Do Some Data Analysis
# Use Case e.g: You need to know how many times a user clicks on an external clicks and if adding a new element helps increase that conversion

# Load and clean Data from a CSV you can download from GA UI
exp_df = pd.read_csv('Path to csv', skiprows=6)

# Append domain if not included in your GA View
# You might have to clean up the URL strings
exp_df['Page'] = 'domain name' + exp_df['Page']

# Loading more data and cleaning
now_df = pd.read_csv('Path to csv', skiprows=6)
now_df['Page'] = 'domain name' + now_df['Page']

# Combining the two dataframes into one
master_df = pd.concat([exp_df, now_df]).dropna().reset_index(drop=True)

# Clean up the dataframe so that you can do some maths functions on it
# Remove ',' so that you can convert the strings to numbers (integers)
master_df = master_df.apply(lambda x: x.str.replace(',', ''))

# Convert string to integers
master_df['Pageviews'] = master_df['Pageviews'].astype(int)

# Sort dataframe by pageviews
master_df = master_df.sort_values(by=['Pageviews'], ascending=False)

# I had some wierd data being pulled, so I wanted to remove empty spaces that could cause issues later on
master_df['Page'] = master_df['Page'].str.replace(' ', '')
url_list = master_df['Page']

# Create a pickle file so you don't have to scrape each time during testing
try:
    with open('html.pickle', 'rb') as f:
        cache = pickle.load(f)
except FileNotFoundError:
    cache = {}


# Go through all URL's from the list and look for correct html Code
# PRO TIP: USE CHROMES INSPECT TO LOOK FOR RELEVENT PART OF PAGE
@cachetools.cached(cache)
def get_html(all_urls):
    # Create a list of all results
    m_list = []
    # Loop through all urls in list
    for things in all_urls:
        # Use a try and except to handle for errors so code doesnt break when running
        try:
            # Get the html from the pages in the list
            test_url = urllib.request.urlopen(things, timeout=10)
            # Create a beautiful soup objetc
            test_url = soup(test_url, features="lxml")
            # Find the relevent div in the html
            mr_div = str(test_url.find(name='div', attrs={'Type of element (e.g: class)':'Name of class'}))
            blog_div = str(test_url.find(name='div', attrs={'Type of element (e.g: class)':'Name of class'}))
            # Create a dictionary with the relevent html
            m_dict = {'url':things, 'html_mr':str(mr_div), 'html_blog': str(blog_div)}
            # make a list of dictionaries so that you can make them into a dataframe easier
            m_list.append(m_dict)
        except:
            continue
    # Make a df from all of the data you've gotten so far
    all_dict = pd.DataFrame(m_list)
    all_dictjson = all_dict.to_json(orient='records')
    return all_dictjson

# You have to store data as a tuple as pickle doesn't support all data types
html_df = get_html(tuple(url_list))

with open('html.pickle', 'wb') as f:
    pickle.dump(cache, f, protocol=pickle.HIGHEST_PROTOCOL)

# Covert tuple to json again and then dataframe for ease of use
m_html = json.loads(html_df)
m_html = pd.DataFrame.from_dict(m_html)

# Function to count how many of a certain element exists on this page, could do anything now
def get_df(df):
    d_list = []
    for index, row in df.iterrows():
        s_count = row['html_mr'].count('relatedContent__item')
        b_count = row['html_blog'].count('years ago')
        s_dict = {'Page': row['url'], 'ph_count': s_count, 'empty/not': 1 if s_count == 0 else 0, 'is_blog':b_count}
        d_list.append(s_dict)
    df_ph = pd.DataFrame(d_list)
    return df_ph

df_parhan = get_df(m_html)
final_df = df_parhan.merge(master_df)


# Some basic functions to print and see the outcome on the df
pv_wmr = final_df.loc[final_df['empty/not'] == 0, 'Pageviews'].sum()
pv_nomr = final_df.loc[final_df['empty/not'] == 1, 'Pageviews'].sum()
print('%empty')
print(pv_nomr/(pv_nomr + (pv_wmr)))
no_mrpages = final_df[final_df['empty/not'] == 1]
yes_mrpages = final_df[final_df['empty/not'] == 0]
no_isb = final_df['Page'][(final_df['empty/not'] == 1) & (final_df['is_blog'] == 0)].count()
no_nob = final_df['Page'][(final_df['empty/not'] == 1) & (final_df['is_blog'] == 1)].count()
print("no PH, not blog")
print(no_isb)
print('no PH, is blog')
print(no_nob)
yes_isb = final_df['Page'][(final_df['empty/not'] == 0) & (final_df['is_blog'] == 0)].count()
yes_nob = final_df['Page'][(final_df['empty/not'] == 0) & (final_df['is_blog'] == 1)].count()
avg_ph = final_df.loc[final_df['empty/not'] == 0, 'ph_count'].mean()
print("yes PH, not blog")
print(yes_isb)
print('yes PH, is blog')
print(yes_nob)
print('average_no')
print(avg_ph)