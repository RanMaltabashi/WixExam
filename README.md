# WixExam
## Overview
This project related to the BI Developer position exam on WIX
## Installation
PLease make sure you have installed all necessary modules
```python
import requests
import json
import pandas as pd
import sqlalchemy as sa
import urllib.parse
from datetime import datetime
```
## Connection details
In order to make mysql connection work please change the details in the connection_details.json file to the correct details
```json
{
"user": "ENTER USER NAME HERE",
"password": "ENTER PASSWORD HERE",
"host": "ENTER HOST HERE",
"port": "ENTER PORT HERE",
"database": "ENTER DATABASE NAME HERE"
}
```
