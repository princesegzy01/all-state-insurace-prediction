import pandas as pd
from tqdm import tqdm
from collections import Counter
import numpy as np
class DataWrangler:
	
	def findMostFrequentChar(self, data):
		return Counter(data).most_common()[0][0]


	def convertToDataframe(self):
    		
		dataPath = "dataset/train.csv"

		fullDataframe = pd.read_csv(dataPath)

		quoteDataframe = fullDataframe[fullDataframe["record_type"] == 0]
		purchaseDataframe = fullDataframe[fullDataframe["record_type"] == 1]

		return fullDataframe, quoteDataframe, purchaseDataframe
	
	def TransformTimeOfTheDay(self, df):
		
		dataframe = df.copy()

		# convert time to array of minute and seconds
		dataframe["splitted_time"] = dataframe["time"].str.split(":")

		#  convert to numpy array so we can select the hour
		x = np.array(dataframe["splitted_time"].tolist())

		# Convert it back to list so we add it to the dataframe
		new_time = list(x[:,0])

		# add new time to dataframe
		dataframe["new_time"] = new_time

		dataframe.loc[dataframe["new_time"].astype('int64') <= 12, 'time_cat'] = ' MORNING'
		dataframe.loc[(dataframe["new_time"].astype('int64') > 12) & (dataframe["new_time"].astype('int64') <= 16), 'time_cat'] = ' AFTERNOON'
		dataframe.loc[dataframe["new_time"].astype('int64') > 16, 'time_cat'] = ' NIGHT'

		return dataframe

	def transformstateToCensusRegion(self, df):
    		
		dataframe = df.copy()
    	
		mid_west = ['IL', 'IN', 'IA', 'KS', 'MI', 'MN', 'MO', 'NE', 'ND', 'OH', 'SD', 'WI']
		north_east = ['CT', 'ME', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT']
		south = ['AL', 'AR', 'DE', 'FL', 'GA', 'KY', 'LA', 'MD', 'MS', 'NC', 'OK', 'SC', 'TN', 'TX', 'VA', 'WV']
		west = ['AK', 'AZ', 'CA', 'CO', 'HI', 'ID', 'MT', 'NV', 'NM', 'OR', 'UT', 'WA', 'WY']
		


		# covert to equivalent censor reqion
		dataframe.loc[dataframe.state.isin(mid_west), 'state'] = 'mid_west'
		dataframe.loc[dataframe.state.isin(north_east), 'state'] = 'north_east'
		dataframe.loc[dataframe.state.isin(south), 'state'] = 'south'
		dataframe.loc[dataframe.state.isin(west), 'state'] = 'west'
		
		return dataframe
	
	def transformAgeToCategorical(self, df):
    		
		dataframe = df.copy()

		# dataframe.loc[(dataframe["age_oldest"].astype('int64') > 16) & (dataframe["age_oldest"].astype('int64') <= 28), 'c_age_oldest'] = 'LOW'
		dataframe.loc[dataframe["age_oldest"].astype('int64') <= 28, 'c_age_oldest'] = 'LOW'
		dataframe.loc[(dataframe["age_oldest"].astype('int64') > 28) & (dataframe["age_oldest"].astype('int64') <= 60), 'c_age_oldest'] = 'MED'
		dataframe.loc[dataframe["age_oldest"].astype('int64') > 60, 'c_age_oldest'] = 'HIGH'

		dataframe.loc[(dataframe["age_youngest"].astype('int64') > 16) & (dataframe["age_youngest"].astype('int64') <= 28), 'c_age_youngest'] = 'LOW'
		dataframe.loc[(dataframe["age_youngest"].astype('int64') > 28) & (dataframe["age_youngest"].astype('int64') <= 60), 'c_age_youngest'] = 'MED'
		dataframe.loc[dataframe["age_youngest"].astype('int64') > 60, 'c_age_youngest'] = 'HIGH'
	
		return dataframe


	def performWeightedAverageOnAG(self, quote_df):

		quote_df['wg_a_cal'] = quote_df.shopping_pt.astype('int64') * quote_df.A.astype('str')
		quote_df['wg_b_cal'] = quote_df.shopping_pt.astype('int64') * quote_df.B.astype('str')
		quote_df['wg_c_cal'] = quote_df.shopping_pt.astype('int64') * quote_df.C.astype('str')
		quote_df['wg_d_cal'] = quote_df.shopping_pt.astype('int64') * quote_df.D.astype('str')
		quote_df['wg_e_cal'] = quote_df.shopping_pt.astype('int64') * quote_df.E.astype('str')
		quote_df['wg_f_cal'] = quote_df.shopping_pt.astype('int64') * quote_df.F.astype('str')
		quote_df['wg_g_cal'] = quote_df.shopping_pt.astype('int64') * quote_df.G.astype('str')

		return quote_df


	def performWeightedAverageOntTime(self, df):
    	
		df['wg_g_time_cal'] = df.shopping_pt.astype('int64') * df.time_cat.astype('str')
		return df

	def save_df_to_directory(self, df, filename):
		df.to_csv("dataset/" + filename , sep='\t', encoding='utf-8')
		print("dataframe successfully saved")


	def summerizeQuote(self,df):
    
		col_names =  ['customer_ID', 'shopping_point', 'day', 'state','group_size','homeowner','car_age','car_value','c_age_oldest','age_youngest','married_couple','c_previous','duration_previous','W_A','W_B','W_C','W_D','W_E','W_F','W_G','cost','wg_time']
		
		uniqueDataframe  = pd.DataFrame(columns = col_names)


		uniqueRecord = []
		print(len(df["customer_ID"].unique()))
		grouped = df.groupby('customer_ID')

		index  = 1
		for name, group in tqdm(grouped):
			# print(name)

			# print(type(name))
			#print(group)
			
			customer_ID = group["customer_ID"].iloc[0]
			shopping_pt = len(group["shopping_pt"])
			day = group["day"].iloc[0]
			state = group["state"].iloc[0]
			group_size = group["group_size"].iloc[0]
			homeowner = group["homeowner"].iloc[0]
			car_age = group["car_age"].iloc[0]
			car_value = group["car_value"].iloc[0]
			married_couple = group["married_couple"].iloc[0]
			c_previous = group["C_previous"].iloc[0]
			avg_cost = group["cost"].mean()
			group_size = group["group_size"].iloc[0]
			duration_previous = group["duration_previous"].iloc[0]

			wg_a_cal = group["wg_a_cal"].tolist()
			wg_a_cal = self.findMostFrequentChar(''.join(wg_a_cal))

			wg_b_cal = group["wg_b_cal"].tolist()
			wg_b_cal = self.findMostFrequentChar(''.join(wg_b_cal))

			wg_c_cal = group["wg_c_cal"].tolist()
			wg_c_cal = self.findMostFrequentChar(''.join(wg_c_cal))

			wg_d_cal = group["wg_d_cal"].tolist()
			wg_d_cal = self.findMostFrequentChar(''.join(wg_d_cal))

			wg_e_cal = group["wg_e_cal"].tolist()
			wg_e_cal = self.findMostFrequentChar(''.join(wg_e_cal))

			wg_f_cal = group["wg_f_cal"].tolist()
			wg_f_cal = self.findMostFrequentChar(''.join(wg_f_cal))

			wg_g_cal = group["wg_g_cal"].tolist()
			wg_g_cal = self.findMostFrequentChar(''.join(wg_g_cal))

			c_age_oldest = group["c_age_oldest"].iloc[0]
			
			c_age_youngest = group["c_age_youngest"].iloc[0]

			time_cat = group["time_cat"].tolist()
			time_cat = self.findMostFrequentChar(time_cat)

			uniqueDataframe.loc[index].customer_ID = customer_ID
			uniqueDataframe.loc[index].shopping_point = shopping_pt
			uniqueDataframe.loc[index].day = day
			uniqueDataframe.loc[index].state = state
			uniqueDataframe.loc[index].group_size = group_size
			uniqueDataframe.loc[index].homeowner = homeowner
			uniqueDataframe.loc[index].car_age = car_age
			uniqueDataframe.loc[index].car_value = car_value
			uniqueDataframe.loc[index].c_age_oldest = c_age_oldest
			uniqueDataframe.loc[index].age_youngest = c_age_youngest
			uniqueDataframe.loc[index].married_couple = married_couple
			uniqueDataframe.loc[index].c_previous = c_previous
			uniqueDataframe.loc[index].duration_previous = duration_previous
			uniqueDataframe.loc[index].W_A = wg_a_cal
			uniqueDataframe.loc[index].W_B = wg_b_cal
			uniqueDataframe.loc[index].W_C = wg_c_cal
			uniqueDataframe.loc[index].W_D = wg_d_cal
			uniqueDataframe.loc[index].W_E = wg_e_cal
			uniqueDataframe.loc[index].W_F = wg_f_cal
			uniqueDataframe.loc[index].W_G = wg_g_cal
			uniqueDataframe.loc[index].cost = avg_cost
			uniqueDataframe.loc[index].wg_time = time_cat
			
			index = index + 1
			#print(time_cat)
			# print(group["state"])
			# print(group["customer_ID"])
		print("Done summerizing Datasets")

		
			

Data =  DataWrangler()

f, quote_df, p = Data.convertToDataframe()

df = Data.transformstateToCensusRegion(quote_df)
df = Data.transformAgeToCategorical(df)
df = Data.performWeightedAverageOnAG(df)

df = Data.TransformTimeOfTheDay(df)
df = Data.performWeightedAverageOntTime(df)

# print(df)
df = Data.summerizeQuote(df)
Data.save_df_to_directory(df, "summerized_result.csv")