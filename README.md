# BarResampler
Implementation of bar resamplers for TAQ data

Implementation details:

1. Given a trade file prepare the format of the file for use with all of your other functions.
2. Given a dataframe extract all ticks for a given symbol.
3. Given a dataframe delete any types of trades the users deems unnecessary. You should
be able to pass the function an option to delete a specific trade type. So for example
maybe you pass a Z to the function to delete all out of sequence trades.
4. Given a dataframe reindex the dataframe using the time stamps.
5. Given a dataframe consisting of data from one symbol sample minute bars. The user
should be able to specify the size of the bars as an option.
6. Given a dataframe consisting of data from one symbol sample tick bars. The user should
be able to specify the size of the bars as an option.
7. Given a dataframe consisting of data from one symbol sample volume bars. The user
should be able to specify the size of the bars as an option.
8. Given a dataframe consisting of data from one symbol sample dollar bars. The user
should be able to specify the size of the bars as an option.

The Documentation can be found [here](https://hbk-ronak.github.io/BarResampler/)
