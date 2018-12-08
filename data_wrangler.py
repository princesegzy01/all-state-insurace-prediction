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
		return fullDataframe

	def splitDataFrameToSections(self, df):
		quoteDataframe = df[df["record_type"] == 0]
		purchaseDataframe = df[df["record_type"] == 1]
		fullDataframe = df

		return fullDataframe, quoteDataframe, purchaseDataframe

	def prepareDataForOperation(self, df):
    		return df
    		
	
	def transformTimeOfTheDay(self, df):
		
		dataframe = df.copy()

		# convert time to array of minute and seconds
		# dataframe["splitted_time"] = dataframe["time"].str.split(":")

		#  convert to numpy array so we can select the hour
		# x = np.array(dataframe["splitted_time"].tolist())

		# Convert it back to list so we add it to the dataframe
		# new_time = list(x[:,0])
		
		# dataframe["new_time"] = new_time


		expandedTime = dataframe["time"].str.split(":", n =1, expand = True)

		# add new time to dataframe
		dataframe["new_time"] = expandedTime[0]

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


	def performWeightedAverageOnAG(self, df):

		quote_df = df.copy()
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

	def saveToDirectory(self, df, filename):
		df.to_csv("result/" + filename , sep='\t', encoding='utf-8')
		print("dataframe successfully saved")


	def summerizeQuote(self,df, purchase):
    
		col_names =  ['customer_ID','shopping_pt', 'record_type', 'day', 'state','group_size','homeowner','car_age','car_value','age_oldest','age_youngest','married_couple','C_previous','duration_previous','A','B','C','D','E','F','G','cost','time_cat']
		
		# uniqueDataframe  = pd.DataFrame(columns = col_names, index = range(568240))


		uniqueRecord = []

		grouped = df.groupby('customer_ID')

		# index  = 0
		for name, group in tqdm(grouped):
			
			customer_ID = group["customer_ID"].iloc[0]
			shopping_pt = len(group["shopping_pt"])
			record_type = group["record_type"].iloc[0]
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

		# 	uniqueDataframe.loc[index].customer_ID = customer_ID
		# 	uniqueDataframe.loc[index].shopping_pt = shopping_pt
		# 	uniqueDataframe.loc[index].record_type = record_type
		# 	uniqueDataframe.loc[index].day = day
		# 	uniqueDataframe.loc[index].state = state
		# 	uniqueDataframe.loc[index].group_size = group_size
		# 	uniqueDataframe.loc[index].homeowner = homeowner
		# 	uniqueDataframe.loc[index].car_age = car_age
		# 	uniqueDataframe.loc[index].car_value = car_value
		# 	uniqueDataframe.loc[index].age_oldest = c_age_oldest
		# 	uniqueDataframe.loc[index].age_youngest = c_age_youngest
		# 	uniqueDataframe.loc[index].married_couple = married_couple
		# 	uniqueDataframe.loc[index].C_previous = c_previous
		# 	uniqueDataframe.loc[index].duration_previous = duration_previous
		# 	uniqueDataframe.loc[index].A = wg_a_cal
		# 	uniqueDataframe.loc[index].B = wg_b_cal
		# 	uniqueDataframe.loc[index].C = wg_c_cal
		# 	uniqueDataframe.loc[index].D = wg_d_cal
		# 	uniqueDataframe.loc[index].E = wg_e_cal
		# 	uniqueDataframe.loc[index].F = wg_f_cal
		# 	uniqueDataframe.loc[index].G = wg_g_cal
		# 	uniqueDataframe.loc[index].cost = avg_cost
		# 	uniqueDataframe.loc[index].time_cat = time_cat

			quoteList = []

			quoteList.append(customer_ID)
			quoteList.append(shopping_pt)
			quoteList.append(record_type)
			quoteList.append(day)
			quoteList.append(state)
			quoteList.append(group_size)
			quoteList.append(homeowner)
			quoteList.append(car_age)
			quoteList.append(car_value)
			quoteList.append(c_age_oldest)
			quoteList.append(c_age_youngest)
			quoteList.append(married_couple)
			quoteList.append(c_previous)
			quoteList.append(duration_previous)
			quoteList.append(wg_a_cal)
			quoteList.append(wg_b_cal)
			quoteList.append(wg_c_cal)
			quoteList.append(wg_d_cal)
			quoteList.append(wg_e_cal)
			quoteList.append(wg_f_cal)
			quoteList.append(wg_g_cal)
			quoteList.append(avg_cost)
			quoteList.append(time_cat)

			uniqueRecord.append(quoteList)
			
		# 	index = index + 1
		# 	#print(time_cat)
		# 	# print(group["state"])
		# 	# print(group["customer_ID"])

		print("✔✔  Done summerizing Datasets")

		print("")
		print("")

		print("Merging Purchase datasets to summerized quote, please wait !!! ")


		
		for index, row in tqdm(purchase.iterrows()):
      		# print row['c1'], row['c2']
			purchaseList = []

			purchaseList.append(row['customer_ID'])
			purchaseList.append(row['shopping_pt'])
			purchaseList.append(row['record_type'])
			purchaseList.append(row['day'])
			purchaseList.append(row['state'])
			purchaseList.append(row['group_size'])
			purchaseList.append(row['homeowner'])
			purchaseList.append(row['car_age'])
			purchaseList.append(row['car_value'])
			purchaseList.append(row['c_age_oldest'])
			purchaseList.append(row['c_age_youngest'])
			purchaseList.append(row['married_couple'])
			purchaseList.append(row['C_previous'])
			purchaseList.append(row['duration_previous'])
			purchaseList.append(row['A'])
			purchaseList.append(row['B'])
			purchaseList.append(row['C'])
			purchaseList.append(row['D'])
			purchaseList.append(row['E'])
			purchaseList.append(row['F'])
			purchaseList.append(row['G'])
			purchaseList.append(row['cost'])
			purchaseList.append(row['time_cat'])

			uniqueRecord.append(purchaseList)

			# uniqueDataframe.loc[index].customer_ID = row['customer_ID']
			# uniqueDataframe.loc[index].shopping_pt = row['shopping_pt']
			# uniqueDataframe.loc[index].record_type = row['record_type']
			# uniqueDataframe.loc[index].day = row['day']
			# uniqueDataframe.loc[index].state = row['state']
			# uniqueDataframe.loc[index].group_size = row['group_size']
			# uniqueDataframe.loc[index].homeowner = row['homeowner']
			# uniqueDataframe.loc[index].car_age = row['car_age']
			# uniqueDataframe.loc[index].car_value = row['car_value']
			# uniqueDataframe.loc[index].age_oldest = row['c_age_oldest']
			# uniqueDataframe.loc[index].age_youngest = row['c_age_youngest']
			# uniqueDataframe.loc[index].married_couple = row['married_couple']
			# uniqueDataframe.loc[index].C_previous = row['C_previous']
			# uniqueDataframe.loc[index].duration_previous = row['duration_previous']
			# uniqueDataframe.loc[index].A = row['A']
			# uniqueDataframe.loc[index].B = row['B']
			# uniqueDataframe.loc[index].C = row['C']
			# uniqueDataframe.loc[index].D = row['D']
			# uniqueDataframe.loc[index].E = row['E']
			# uniqueDataframe.loc[index].F = row['F']
			# uniqueDataframe.loc[index].G = row['G']
			# uniqueDataframe.loc[index].cost = row['cost']
			# uniqueDataframe.loc[index].time_cat = row['time_cat']



		dfIndex = range(len(uniqueRecord))

		uniqueDataframe  = pd.DataFrame(uniqueRecord, columns = col_names, index = dfIndex)
		print("✔✔ Purchase successfully merged")

		return uniqueDataframe
		

	def mergeDataframe(self, purchase, quote):
    		
			#  drop some fields in purchase datafram
			purchase.drop(["time","location","new_time","risk_factor"], axis=1)

			frame = [ purchase, quote]
			joinedDF = pd.concat([quote, purchase], axis=0, sort=True)

			# joinedDF.reindex(range(len(joinedDF["customer_ID"])))

			return joinedDF


			
# initialize class
Data =  DataWrangler()

print("")
print("")
print(" Starting data preparation & compression , please wait !!! ")
print("")

# convert csv file to dataframe
df = Data.convertToDataframe()

# process non available data in dataframe
df = Data.prepareDataForOperation(df)

# convert and group state to 4 categories
df = Data.transformstateToCensusRegion(df)

#  transform age from numeric to categorical
df = Data.transformAgeToCategorical(df)


# transform the time of quote from time to Categorical
df = Data.transformTimeOfTheDay(df)

# split dataframe to all, putchase & quote
full, quote_df, purchase = Data.splitDataFrameToSections(df)
# Data.saveToDirectory(purchase, "purchase_result.csv")

# perform weighted average on the A-G features
df = Data.performWeightedAverageOnAG(quote_df)
# perform weighted average on time
df = Data.performWeightedAverageOntTime(df)

#  summerize duplicates quotes to one for customer ID
quote = Data.summerizeQuote(df, purchase)


# result = Data.mergeDataframe(purchase, quote)

Data.saveToDirectory(quote, "summerized_quote.csv")
