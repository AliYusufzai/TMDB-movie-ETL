# TMDB-movie-ETL
fetching the data using TMDB API and then using pandas to clean the data.
cherry pick the columns which we need.
saving those cherry picked data into csv



```python
#import necessary libraries

import pandas as pd
import requests
import os
```


```python
#create enviornment variable to store API KEY
key = os.environ.get('API_KEY')
```


```python
#we send a single GET request to the API. In the response, we receive a JSON record with the movie_id

response_list = []

for movie_db in range(550,556):
    url = 'https://api.themoviedb.org/3/movie/{}?api_key={}'.format(movie_db, key)
    r = requests.get(url)
    response_list.append(r.json())
```


```python
#list of long, unwieldy JSON records delivered to us from the API. Create a pandas dataframe from the records using from_dict()

df = pd.DataFrame.from_dict(response_list)
```


```python
df
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>adult</th>
      <th>backdrop_path</th>
      <th>belongs_to_collection</th>
      <th>budget</th>
      <th>genres</th>
      <th>homepage</th>
      <th>id</th>
      <th>imdb_id</th>
      <th>original_language</th>
      <th>original_title</th>
      <th>...</th>
      <th>release_date</th>
      <th>revenue</th>
      <th>runtime</th>
      <th>spoken_languages</th>
      <th>status</th>
      <th>tagline</th>
      <th>title</th>
      <th>video</th>
      <th>vote_average</th>
      <th>vote_count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>False</td>
      <td>/rr7E0NoGKxvbkb89eR1GwfoYjpA.jpg</td>
      <td>None</td>
      <td>63000000</td>
      <td>[{'id': 18, 'name': 'Drama'}, {'id': 53, 'name...</td>
      <td>http://www.foxmovies.com/movies/fight-club</td>
      <td>550</td>
      <td>tt0137523</td>
      <td>en</td>
      <td>Fight Club</td>
      <td>...</td>
      <td>1999-10-15</td>
      <td>100853753</td>
      <td>139</td>
      <td>[{'english_name': 'English', 'iso_639_1': 'en'...</td>
      <td>Released</td>
      <td>Mischief. Mayhem. Soap.</td>
      <td>Fight Club</td>
      <td>False</td>
      <td>8.434</td>
      <td>24980</td>
    </tr>
    <tr>
      <th>1</th>
      <td>False</td>
      <td>/v1QEIuBM1vvpvfqalahhIyXY0Cm.jpg</td>
      <td>{'id': 372257, 'name': 'The Poseidon Adventure...</td>
      <td>5000000</td>
      <td>[{'id': 28, 'name': 'Action'}, {'id': 12, 'nam...</td>
      <td></td>
      <td>551</td>
      <td>tt0069113</td>
      <td>en</td>
      <td>The Poseidon Adventure</td>
      <td>...</td>
      <td>1972-12-13</td>
      <td>84563118</td>
      <td>117</td>
      <td>[{'english_name': 'English', 'iso_639_1': 'en'...</td>
      <td>Released</td>
      <td>Hell, upside down.</td>
      <td>The Poseidon Adventure</td>
      <td>False</td>
      <td>7.142</td>
      <td>680</td>
    </tr>
    <tr>
      <th>2</th>
      <td>False</td>
      <td>/k4JIHyAXaGHwAwT7y5Skd17f0Wl.jpg</td>
      <td>None</td>
      <td>0</td>
      <td>[{'id': 35, 'name': 'Comedy'}, {'id': 10749, '...</td>
      <td></td>
      <td>552</td>
      <td>tt0237539</td>
      <td>it</td>
      <td>Pane e tulipani</td>
      <td>...</td>
      <td>2000-03-03</td>
      <td>8478434</td>
      <td>114</td>
      <td>[{'english_name': 'Italian', 'iso_639_1': 'it'...</td>
      <td>Released</td>
      <td>Imagine your life. Now go live it.</td>
      <td>Bread and Tulips</td>
      <td>False</td>
      <td>7.298</td>
      <td>223</td>
    </tr>
    <tr>
      <th>3</th>
      <td>False</td>
      <td>/r3xsFBD1VTUusk393bBc7SsDUJe.jpg</td>
      <td>{'id': 1952, 'name': 'USA: Land of Opportuniti...</td>
      <td>10000000</td>
      <td>[{'id': 80, 'name': 'Crime'}, {'id': 18, 'name...</td>
      <td></td>
      <td>553</td>
      <td>tt0276919</td>
      <td>en</td>
      <td>Dogville</td>
      <td>...</td>
      <td>2003-05-19</td>
      <td>16680836</td>
      <td>178</td>
      <td>[{'english_name': 'English', 'iso_639_1': 'en'...</td>
      <td>Released</td>
      <td>A quiet little town not far from here.</td>
      <td>Dogville</td>
      <td>False</td>
      <td>7.796</td>
      <td>2005</td>
    </tr>
    <tr>
      <th>4</th>
      <td>False</td>
      <td>/1qwXItFKqvKYyW1CwbYhxyUC8Pj.jpg</td>
      <td>None</td>
      <td>0</td>
      <td>[{'id': 18, 'name': 'Drama'}, {'id': 36, 'name...</td>
      <td></td>
      <td>554</td>
      <td>tt0308476</td>
      <td>ru</td>
      <td>Кукушка</td>
      <td>...</td>
      <td>2002-01-01</td>
      <td>0</td>
      <td>100</td>
      <td>[{'english_name': 'German', 'iso_639_1': 'de',...</td>
      <td>Released</td>
      <td>She's Making Peace One Man at a Time.</td>
      <td>The Cuckoo</td>
      <td>False</td>
      <td>7.045</td>
      <td>66</td>
    </tr>
    <tr>
      <th>5</th>
      <td>False</td>
      <td>None</td>
      <td>None</td>
      <td>0</td>
      <td>[{'id': 53, 'name': 'Thriller'}]</td>
      <td>http://www.luecke-im-system.de/</td>
      <td>555</td>
      <td>tt0442896</td>
      <td>en</td>
      <td>Absolut</td>
      <td>...</td>
      <td>2005-04-20</td>
      <td>0</td>
      <td>94</td>
      <td>[{'english_name': 'French', 'iso_639_1': 'fr',...</td>
      <td>Released</td>
      <td></td>
      <td>Absolut</td>
      <td>False</td>
      <td>7.786</td>
      <td>21</td>
    </tr>
  </tbody>
</table>
<p>6 rows × 25 columns</p>
</div>




```python
df.shape
df.info()
```

    <class 'pandas.core.frame.DataFrame'>
    RangeIndex: 6 entries, 0 to 5
    Data columns (total 25 columns):
     #   Column                 Non-Null Count  Dtype  
    ---  ------                 --------------  -----  
     0   adult                  6 non-null      bool   
     1   backdrop_path          5 non-null      object 
     2   belongs_to_collection  2 non-null      object 
     3   budget                 6 non-null      int64  
     4   genres                 6 non-null      object 
     5   homepage               6 non-null      object 
     6   id                     6 non-null      int64  
     7   imdb_id                6 non-null      object 
     8   original_language      6 non-null      object 
     9   original_title         6 non-null      object 
     10  overview               6 non-null      object 
     11  popularity             6 non-null      float64
     12  poster_path            6 non-null      object 
     13  production_companies   6 non-null      object 
     14  production_countries   6 non-null      object 
     15  release_date           6 non-null      object 
     16  revenue                6 non-null      int64  
     17  runtime                6 non-null      int64  
     18  spoken_languages       6 non-null      object 
     19  status                 6 non-null      object 
     20  tagline                6 non-null      object 
     21  title                  6 non-null      object 
     22  video                  6 non-null      bool   
     23  vote_average           6 non-null      float64
     24  vote_count             6 non-null      int64  
    dtypes: bool(2), float64(2), int64(5), object(16)
    memory usage: 1.2+ KB
    


```python
#We create a list of column names called df_columns that allows us to select the columns we want from the main dataframe.

df_columns = ['budget', 'genres', 'id', 'imddb_id', 'original_title', 'release_date', 'revenue', 'runtime']
df_columns
```




    ['budget',
     'genres',
     'id',
     'imddb_id',
     'original_title',
     'release_date',
     'revenue',
     'runtime']




```python
#We’ll only be using the name property, not the id

genres_list = df['genres'].tolist()
flat_list = [item for sublist in genres_list for item in sublist]
```


```python
#We’ll create a temporary column called genres_all as a list of lists of genres that we can later expand out into a
#separate column

result = []
for i in genres_list:
    r = []
    for d in i:
        r.append(d['name'])
    result.append(r)

df = df.assign(genres_all = result)
```


```python
#we create the genres table

df_genres = pd.DataFrame.from_records(flat_list).drop_duplicates()
```


```python
df_genres
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>name</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>18</td>
      <td>Drama</td>
    </tr>
    <tr>
      <th>1</th>
      <td>53</td>
      <td>Thriller</td>
    </tr>
    <tr>
      <th>2</th>
      <td>35</td>
      <td>Comedy</td>
    </tr>
    <tr>
      <th>3</th>
      <td>28</td>
      <td>Action</td>
    </tr>
    <tr>
      <th>4</th>
      <td>12</td>
      <td>Adventure</td>
    </tr>
    <tr>
      <th>7</th>
      <td>10749</td>
      <td>Romance</td>
    </tr>
    <tr>
      <th>8</th>
      <td>80</td>
      <td>Crime</td>
    </tr>
    <tr>
      <th>12</th>
      <td>36</td>
      <td>History</td>
    </tr>
  </tbody>
</table>
</div>




```python
#We attach the list of genre names onto our df_columns list

df_columns = ['budget', 'id', 'imdb_id', 'original_title', 'release_date', 'revenue', 'runtime']
df_genre_columns = df_genres['name'].to_list()
df_columns.extend(df_genre_columns)

s = df['genres_all'].explode()
df = df.join(pd.crosstab(s.index, s))
```


    ---------------------------------------------------------------------------

    ValueError                                Traceback (most recent call last)

    Input In [24], in <cell line: 6>()
          3 df_columns.extend(df_genre_columns)
          5 s = df['genres_all'].explode()
    ----> 6 df = df.join(pd.crosstab(s.index, s))
    

    File ~\AppData\Local\Programs\Python\Python310\lib\site-packages\pandas\core\frame.py:9969, in DataFrame.join(self, other, on, how, lsuffix, rsuffix, sort, validate)
       9806 def join(
       9807     self,
       9808     other: DataFrame | Series | list[DataFrame | Series],
       (...)
       9814     validate: str | None = None,
       9815 ) -> DataFrame:
       9816     """
       9817     Join columns of another DataFrame.
       9818 
       (...)
       9967     5  K1  A5   B1
       9968     """
    -> 9969     return self._join_compat(
       9970         other,
       9971         on=on,
       9972         how=how,
       9973         lsuffix=lsuffix,
       9974         rsuffix=rsuffix,
       9975         sort=sort,
       9976         validate=validate,
       9977     )
    

    File ~\AppData\Local\Programs\Python\Python310\lib\site-packages\pandas\core\frame.py:10008, in DataFrame._join_compat(self, other, on, how, lsuffix, rsuffix, sort, validate)
       9998     if how == "cross":
       9999         return merge(
      10000             self,
      10001             other,
       (...)
      10006             validate=validate,
      10007         )
    > 10008     return merge(
      10009         self,
      10010         other,
      10011         left_on=on,
      10012         how=how,
      10013         left_index=on is None,
      10014         right_index=True,
      10015         suffixes=(lsuffix, rsuffix),
      10016         sort=sort,
      10017         validate=validate,
      10018     )
      10019 else:
      10020     if on is not None:
    

    File ~\AppData\Local\Programs\Python\Python310\lib\site-packages\pandas\core\reshape\merge.py:125, in merge(left, right, how, on, left_on, right_on, left_index, right_index, sort, suffixes, copy, indicator, validate)
         94 @Substitution("\nleft : DataFrame or named Series")
         95 @Appender(_merge_doc, indents=0)
         96 def merge(
       (...)
        109     validate: str | None = None,
        110 ) -> DataFrame:
        111     op = _MergeOperation(
        112         left,
        113         right,
       (...)
        123         validate=validate,
        124     )
    --> 125     return op.get_result(copy=copy)
    

    File ~\AppData\Local\Programs\Python\Python310\lib\site-packages\pandas\core\reshape\merge.py:778, in _MergeOperation.get_result(self, copy)
        774     self.left, self.right = self._indicator_pre_merge(self.left, self.right)
        776 join_index, left_indexer, right_indexer = self._get_join_info()
    --> 778 result = self._reindex_and_concat(
        779     join_index, left_indexer, right_indexer, copy=copy
        780 )
        781 result = result.__finalize__(self, method=self._merge_type)
        783 if self.indicator:
    

    File ~\AppData\Local\Programs\Python\Python310\lib\site-packages\pandas\core\reshape\merge.py:732, in _MergeOperation._reindex_and_concat(self, join_index, left_indexer, right_indexer, copy)
        729 left = self.left[:]
        730 right = self.right[:]
    --> 732 llabels, rlabels = _items_overlap_with_suffix(
        733     self.left._info_axis, self.right._info_axis, self.suffixes
        734 )
        736 if left_indexer is not None:
        737     # Pinning the index here (and in the right code just below) is not
        738     #  necessary, but makes the `.take` more performant if we have e.g.
        739     #  a MultiIndex for left.index.
        740     lmgr = left._mgr.reindex_indexer(
        741         join_index,
        742         left_indexer,
       (...)
        747         use_na_proxy=True,
        748     )
    

    File ~\AppData\Local\Programs\Python\Python310\lib\site-packages\pandas\core\reshape\merge.py:2461, in _items_overlap_with_suffix(left, right, suffixes)
       2458 lsuffix, rsuffix = suffixes
       2460 if not lsuffix and not rsuffix:
    -> 2461     raise ValueError(f"columns overlap but no suffix specified: {to_rename}")
       2463 def renamer(x, suffix):
       2464     """
       2465     Rename the left and right indices.
       2466 
       (...)
       2477     x : renamed column name
       2478     """
    

    ValueError: columns overlap but no suffix specified: Index(['Action', 'Adventure', 'Comedy', 'Crime', 'Drama', 'History', 'Romance',
           'Thriller'],
          dtype='object')



```python
df[df_columns]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>budget</th>
      <th>id</th>
      <th>imdb_id</th>
      <th>original_title</th>
      <th>release_date</th>
      <th>revenue</th>
      <th>runtime</th>
      <th>Drama</th>
      <th>Thriller</th>
      <th>Comedy</th>
      <th>Action</th>
      <th>Adventure</th>
      <th>Romance</th>
      <th>Crime</th>
      <th>History</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>63000000</td>
      <td>550</td>
      <td>tt0137523</td>
      <td>Fight Club</td>
      <td>1999-10-15</td>
      <td>100853753</td>
      <td>139</td>
      <td>1</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>5000000</td>
      <td>551</td>
      <td>tt0069113</td>
      <td>The Poseidon Adventure</td>
      <td>1972-12-13</td>
      <td>84563118</td>
      <td>117</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0</td>
      <td>552</td>
      <td>tt0237539</td>
      <td>Pane e tulipani</td>
      <td>2000-03-03</td>
      <td>8478434</td>
      <td>114</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>10000000</td>
      <td>553</td>
      <td>tt0276919</td>
      <td>Dogville</td>
      <td>2003-05-19</td>
      <td>16680836</td>
      <td>178</td>
      <td>1</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0</td>
      <td>554</td>
      <td>tt0308476</td>
      <td>Кукушка</td>
      <td>2002-01-01</td>
      <td>0</td>
      <td>100</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>1</td>
    </tr>
    <tr>
      <th>5</th>
      <td>0</td>
      <td>555</td>
      <td>tt0442896</td>
      <td>Absolut</td>
      <td>2005-04-20</td>
      <td>0</td>
      <td>94</td>
      <td>0</td>
      <td>1</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
</div>




```python
#Finally we’ll expand out the datetime column into a table

df['release_date'] = pd.to_datetime(df['release_date'])
df['day'] = df['release_date'].dt.day
df['month'] = df['release_date'].dt.month
df['year'] = df['release_date'].dt.year
df['day_of_week'] = df['release_date'].dt.day_name()
df_time_columns = ['id', 'release_date', 'day', 'month', 'year', 'day_of_week']
```


```python
df[df_time_columns]
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>release_date</th>
      <th>day</th>
      <th>month</th>
      <th>year</th>
      <th>day_of_week</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>550</td>
      <td>1999-10-15</td>
      <td>15</td>
      <td>10</td>
      <td>1999</td>
      <td>Friday</td>
    </tr>
    <tr>
      <th>1</th>
      <td>551</td>
      <td>1972-12-13</td>
      <td>13</td>
      <td>12</td>
      <td>1972</td>
      <td>Wednesday</td>
    </tr>
    <tr>
      <th>2</th>
      <td>552</td>
      <td>2000-03-03</td>
      <td>3</td>
      <td>3</td>
      <td>2000</td>
      <td>Friday</td>
    </tr>
    <tr>
      <th>3</th>
      <td>553</td>
      <td>2003-05-19</td>
      <td>19</td>
      <td>5</td>
      <td>2003</td>
      <td>Monday</td>
    </tr>
    <tr>
      <th>4</th>
      <td>554</td>
      <td>2002-01-01</td>
      <td>1</td>
      <td>1</td>
      <td>2002</td>
      <td>Tuesday</td>
    </tr>
    <tr>
      <th>5</th>
      <td>555</td>
      <td>2005-04-20</td>
      <td>20</td>
      <td>4</td>
      <td>2005</td>
      <td>Wednesday</td>
    </tr>
  </tbody>
</table>
</div>




```python
#We ended up creating 3 tables for the tmdb schema

df[df_columns].to_csv('tmdb_movies.csv', index=False)
df_genres.to_csv('tmdb_genres.csv', index=False)
df[df_time_columns].to_csv('tmdb_datetimes.csv', index=False)
```


```python

```


