

from socket import TIPC_SRC_DROPPABLE


class TescoParser:
    """A class that handles the parsing and storage 
    """

    # ### structure
    # file
    # |-Customer data
    # |- Order (online)
    # | |-array 1   - 5 elements - one has days in the dev set
    # |   |-array 2 - 9 elements
    # |     |- order details.
    # |- Purchase
    #   |- array 1                          
    #     |- array 2                        # 7 arrays - 517, 94, 0, 0, 0, 0, 0 (in the dev dataset)
    #       |- shopping trip details  - each of the arrays contain 1 trip - 12 keys - one is 
    #         |-products
    #
    #

    def __init__(self, filepath):
        import json
        self.filepath=filepath

        # this errors if the file doesn't exist or if it's not 
        with open(filepath, "r") as f:
            self.fulljs = json.load(f)

    def parse(self):
        """Initiates the parsing of the dataframe. acts on the data parsed at init
        Return: tuple of 3 dataframes
         * online orders (no product information, just item quantities)
         * trips - details of shopping trips i.e. till transactions)
         * products - the itemisation relating to the trips - these have the common field of "trip_key"
         the above are all saved as instance variable
         """
        online_orders = self.parse_tesco_orders()
        trips, products = self.parse_tesco_purchase()
        return online_orders, trips, products

            

    ################## Orders

    def parse_tesco_orders(self):
        """
        Accepts: None - uses a json structure parsed at init (i.e. the output of a json.loads) from Tesco 
        Return: a tuple with two dataframes: 
         * A dataframe of trips (i.e. purchases at the till) 
         * A dataframs of items purchased.
        These have a common reference of trip_ke
        """
        import json

        import pandas as pd
        fulljs = self.fulljs
        orders = fulljs["Order"]

        online_orders = pd.DataFrame()
        for ix, item in enumerate(orders):
        #skip if there is nothing to do.
            if len(item) == 0:
                continue 
        
        # parse dataframe
            df = pd.read_json(json.dumps(item))
            df["order_index"] = ix
            online_orders = pd.concat([online_orders, df])
            self.online_orders = online_orders
        return online_orders

    def parse_tesco_purchase(self):
        """
        Accepts: None - uses a json structure parsed at init (i.e. the output of a json.loads) from Tesco 
        Return: a tuple with two dataframes: 
         * A dataframe of trips (i.e. purchases at the till) 
         * A dataframs of items purchased.
        These have a common reference of trip_ke
        """

        # The aim is to parse the shopping trip details into a "trip" table and put "products" in a referenced table.
        # i will combine the index from array1, array2 (the trip) & the product index by multiplying by 10000, 
        

        #this is the basic commands to get pandas to parse the json structure to 
        #purch = fulljs["Purchase"] #creates purchase 
        #df = pd.read_json(json.dumps(purch[1]))                                      # df is the trips
        #df["product"].apply(lambda x: pd.read_json(json.dumps(x)))[90]               # the df is the producs form trip 90
        import json

        import pandas as pd

        fulljs = self.fulljs
        purch = fulljs["Purchase"] #creates purchase 

        # loop through the first array creating DF for each item and writing them to trips - parse out products and append 
        trips = pd.DataFrame()
        products = pd.DataFrame()

        for ix, item in enumerate(purch):
            
            #skip if there is nothing to do.
            if len(item) == 0:
                continue 

            
            # parse dataframe
            df = pd.read_json(json.dumps(item))
            #df["i_1"] = ix
            
            # index 
            ix *= 10000 #
            df["trip_key"] = ix + df.index  

            #append to trips df
            trips = pd.concat([trips, df.drop("product", axis=1)]) 
            

            

            #parse the json string in the product ot a Dataframe - giving us a DF inside a DF
            df["product_df"] = df["product"].apply(lambda x: pd.read_json(json.dumps(x)))
            
            # change the Series of DataFrames to a list of dataframes which then works well with pd.concat.
            prod_df = pd.concat(list(df["product_df"]), 
                keys=list(df["trip_key"]),                     # use the keys to pass through the index for the trip to maintain referencial integrity
                names=["trip", "Item"]).reset_index()            # name the indexs and reset index so that it pushes into columns.
            
            #then lats stick this onto the products stack.
            products = pd.concat([products, prod_df])
        
        #instance variables
        self.products = products
        self.trips = trips

        return trips, products



if __name__ == "__main__":
        import sys

        # check arguments
        arg = sys.argv[1:]

        # if no argument - do nothing
        if len(arg) == 0:
            print("Please provide a filename as an argument in the commandline.")
        elif len(arg) == 1:
            filepath = arg[0]
            # if it doesn't end .json - do nothing
            if filepath[-5:].lower() != ".json":
                print("files should end in .json, it ends in {}".format(filepath[:-5]))
            else:
                # init parser, parse and collect the 3 dataframes
                filepath_out = filepath[:-5] + "_{}.csv"
                parser = TescoParser(filepath)
                online_orders, trips, products = parser.parse()

                # push the dataframes to disk at the pwd
                online_filename = filepath_out.format("online_orders")
                print("Saving {}...".format(online_filename))
                online_orders.to_csv(online_filename, index=False)

                trips_filename = filepath_out.format("trips")
                print("Saving {}...".format(trips_filename))
                trips.to_csv(trips_filename, index=False)
                
                products_filename = filepath_out.format("products")
                print("Saving {}...".format(products_filename))
                products.to_csv(products_filename, index=False)
                
                





