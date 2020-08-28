# funneljoin-py
A python port of funneljoin, by Emily Robinson

## Examples


```python
from funneljoin import after_join, get_example_data

landed, registered = get_example_data()
```


```python
landed
```




<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>timestamp</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2018-07-01</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>2018-07-01</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>2018-07-02</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>2018-07-01</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>2018-07-04</td>
    </tr>
    <tr>
      <th>5</th>
      <td>5</td>
      <td>2018-07-10</td>
    </tr>
    <tr>
      <th>6</th>
      <td>5</td>
      <td>2018-07-12</td>
    </tr>
    <tr>
      <th>7</th>
      <td>6</td>
      <td>2018-07-07</td>
    </tr>
    <tr>
      <th>8</th>
      <td>6</td>
      <td>2018-07-08</td>
    </tr>
  </tbody>
</table>
<p>9 rows × 2 columns</p>




```python
registered
```




<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>timestamp</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2018-07-02</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
      <td>2018-07-02</td>
    </tr>
    <tr>
      <th>2</th>
      <td>4</td>
      <td>2018-06-10</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>2018-07-02</td>
    </tr>
    <tr>
      <th>4</th>
      <td>5</td>
      <td>2018-07-11</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>2018-07-10</td>
    </tr>
    <tr>
      <th>6</th>
      <td>6</td>
      <td>2018-07-11</td>
    </tr>
    <tr>
      <th>7</th>
      <td>7</td>
      <td>2018-07-07</td>
    </tr>
  </tbody>
</table>
<p>8 rows × 2 columns</p>




```python
after_join(
    landed, registered,
    by_user = "user_id", by_time = "timestamp",
    mode = "inner", type = "any-firstafter"
    )
```




<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>user_id</th>
      <th>timestamp_x</th>
      <th>timestamp_y</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>2018-07-01</td>
      <td>2018-07-02</td>
    </tr>
    <tr>
      <th>1</th>
      <td>3</td>
      <td>2018-07-02</td>
      <td>2018-07-02</td>
    </tr>
    <tr>
      <th>2</th>
      <td>4</td>
      <td>2018-07-01</td>
      <td>2018-07-02</td>
    </tr>
    <tr>
      <th>3</th>
      <td>5</td>
      <td>2018-07-10</td>
      <td>2018-07-11</td>
    </tr>
    <tr>
      <th>4</th>
      <td>6</td>
      <td>2018-07-07</td>
      <td>2018-07-10</td>
    </tr>
    <tr>
      <th>5</th>
      <td>6</td>
      <td>2018-07-08</td>
      <td>2018-07-10</td>
    </tr>
  </tbody>
</table>
<p>6 rows × 3 columns</p>


