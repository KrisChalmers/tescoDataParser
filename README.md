# tescoDataParser
A Class and script that parses the JSON file that is provided from Tesco personal data portability request. Data is parsed to 3 csv files. 
* online orders (no product information, just item quantities)
* trips - details of shopping trips i.e. till transactions)
* products - the itemisation relating to the trips - these have the common field of "trip_key" the above are all saved as instance variable

## Running from commandline
This can be run from the commandline as follows:

    python parser.py Tesco-Customer-Data.json

## Use in python
The class can be used as follows:
    from parser import TescoParser
    filepath = "Tesco-Customer-Data.json"
    parser = TescoParser(filepath)
    online_orders, trips, products = parser.parse()

