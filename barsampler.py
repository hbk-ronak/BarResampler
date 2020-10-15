import pandas as pd

def prepare_data(filepath):
    """
        Given a file name convert the data into a pipe separated csv.
        
        Parameters
        ----------
        filepath: str
            Location and name of flatfile extracted from gz.

        Returns
        ----------
            None

    """
    
    with open(filepath, "r") as f:
        file = f.readlines()

    file.pop()

    with open(filepath+".csv", "w") as f:
        for i in range(len(file)):
            if i%1000 == 0:
                f.write(file[i])
    return


def get_ticks_symbols(data,symbol):
    """
        Filters the data for a particular symbol.
        
        Parameters
        ----------
        data: pandas.core.frame.DataFrame.
            Raw Data
        symbol: str
            Symbol of the ticker.

        Returns
        ----------
        pandas.core.frame.DataFrame
            Dataframe containing data for one symbol.
    
    """
    return data[data["Symbol"]==symbol]


def delete_trade_type(data, trade_type):
    """
        Removes trade of a particular type.

        Parameters
        ----------
        data: pandas.core.frame.DataFrame
            Raw data or data from one. 
        symbol: str
            Symbol of the ticker.

        Returns
        ----------
        pandas.core.frame.DataFrame
            Dataframe containing data for one symbol. 

    """
    
    return data[data["Sale Condition"].str.find(trade_type) < 0]

def reindex_time(data):
    """
        Reindexes dataframe with the timestamp of the trade.
        
        Parameters
        ----------
        data: pandas.core.frame.DataFrame
            Raw data or data from one .
        symbol: str
            Symbol of the ticker.

        Returns
        ----------
        pandas.core.frame.DataFrame
            Dataframe indexed with timestamp.

    """
    
    data["Time"] = pd.to_datetime(data["Time"], format = '%H%M%S%f')
    data = data.set_index("Time")
    data.index.names = ["Time"]
    return data

def min_resampler(data, units = "T", size = 15, volume = False):
    """
        Resamples the data based on time period.
        
        Parameters
        ----------
        data: pandas.core.frame.DataFrame
            Raw data or data from one .
        units: str
            Unit of the time period {"T": Minutes, "S": Seconds}.
        size: int
            No of units.
        volume: bool
            Produce volume in the final output. 
        
        Returns
        ----------
        pandas.core.frame.DataFrame
            Dataframe containing OHLC(V). 

    """
    
    data = data[(data.index <= '1900-01-01 16:30:00.0')& (data.index >= '1900-01-01 09:30:00.0')]
    window = str(size)+units
    if not volume:
        return data['Trade Price'].resample(window).ohlc()
    else:
        return data['Trade Price'].resample(window).ohlc().merge(
            data['Trade Volume'].resample(window).sum(), 
            how = "inner", left_index = True, right_index = True)
    
def tick_resampler(data, size, volume = False):
    """
        Resamples the data based on Ticks.
        
        Parameters
        ----------
        data: pandas.core.frame.DataFrame
            Raw data or data from one .
        size: int
            Number of ticks to combine.
        volume: bool
            Produce volume in the final output. 
        
        Returns
        ----------
        pandas.core.frame.DataFrame
            Dataframe containing OHLC(V).

    """
    
    data = data[(data.index <= '1900-01-01 16:30:00.0')& (data.index >= '1900-01-01 09:30:00.0')]
    start = 0
    ohlc = []
    while(start<data.shape[0]):
        temp = data['Trade Price'][start:start+size]
        o,h,l,c,v= temp[0],max(temp), min(temp), temp[-1], sum(data['Trade Volume'][start:start+size])
        ohlc.append((o,h,l,c,v))
        start += size
    if volume:
        return pd.DataFrame(ohlc, columns = ["Open", "High", "Low", "Close", "volume"])
    else:
        return pd.DataFrame(ohlc, columns = ["Open", "High", "Low", "Close", "volume"]).drop(["volume"],axis = 1)
    
def volume_resampler(data, size=1000):
    """
        Resamples the data based on Volume.
        
        Parameters
        ----------
        data: pandas.core.frame.DataFrame
            Raw data or data from one .
        size: int
            Run total of the volume.

        Returns
        ----------
        pandas.core.frame.DataFrame
            Dataframe containing OHLCV. 

    """
    
    data = data[(data.index <= '1900-01-01 16:30:00.0')& (data.index >= '1900-01-01 09:30:00.0')]
    vol_sum = 0
    start = 0
    ohlc = []
    for i in range(data.shape[0]):
        if vol_sum+data['Trade Volume'][i] < size:
            vol_sum+=data['Trade Volume'][i]
        else:
            ohlc.append((data['Trade Price'][start], max(data['Trade Price'][start:i+1]), 
                        min(data['Trade Price'][start:i+1]), data['Trade Price'][i], sum(data['Trade Volume'][start:i+1])))
            vol_sum = 0
            start = i+1
    return pd.DataFrame(ohlc, columns = ["Open", "High", "Low", "Close", "Volume"])

def dollar_resampler(data, size=50000):
    """
        Resamples the data based on Dollars traded.
        
        Dollars = price * volume.
        
        Parameters
        ----------
        data: pandas.core.frame.DataFrame
            Data for one tick .
        size: int
            Run total of Dollars traded.
        
        Returns
        ----------
            pandas.core.frame.DataFrame
                Dataframe containing OHLCV .

    """
    
    data = data[(data.index <= '1900-01-01 16:30:00.0')& (data.index >= '1900-01-01 09:30:00.0')]
    dollar_sum = 0
    start = 0
    ohlc = []
    for i in range(data.shape[0]):
        if dollar_sum+(data['Trade Price'][i] * data['Trade Volume'][i]) < size:
            dollar_sum+=(data['Trade Price'][i] * data['Trade Volume'][i])
        else:
            ohlc.append((data['Trade Price'][start], max(data['Trade Price'][start:i+1]), 
                        min(data['Trade Price'][start:i+1]), data['Trade Price'][i], sum(data['Trade Volume'][start:i+1])
                         ,sum(data['Trade Volume'][start:i+1]*data['Trade Price'][start:i+1])))
            dollar_sum = 0
            start = i+1
    return pd.DataFrame(ohlc, columns = ["Open", "High", "Low", "Close", "Volume", "Dollar"])
