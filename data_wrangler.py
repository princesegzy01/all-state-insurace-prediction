import pandas as pd
from tqdm import tqdm

class DataWrangler:

	def convertToDataframe(self):
    		
		dataPath = "dataset/train.csv"

		fullDataframe = pd.read_csv(dataPath)

		quoteDataframe = fullDataframe[fullDataframe["record_type"] == 0]
		purchaseDataframe = fullDataframe[fullDataframe["record_type"] == 1]

		return fullDataframe, quoteDataframe, purchaseDataframe
	
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

		dataframe.loc[(dataframe["age_oldest"].astype('int64') > 16) & (dataframe["age_oldest"].astype('int64') <= 28), 'c_age_oldest'] = 'LOW'
		dataframe.loc[(dataframe["age_oldest"].astype('int64') > 28) & (dataframe["age_oldest"].astype('int64') <= 60), 'c_age_oldest'] = 'MED'
		dataframe.loc[dataframe["age_oldest"].astype('int64') > 60, 'c_age_oldest'] = 'HIGH'

		dataframe.loc[(dataframe["age_youngest"].astype('int64') > 16) & (dataframe["age_youngest"].astype('int64') <= 28), 'c_age_youngest'] = 'LOW'
		dataframe.loc[(dataframe["age_youngest"].astype('int64') > 28) & (dataframe["age_youngest"].astype('int64') <= 60), 'c_age_youngest'] = 'MED'
		dataframe.loc[dataframe["age_youngest"].astype('int64') > 60, 'c_age_youngest'] = 'HIGH'
	
		return dataframe


	def performWeightedAverage(self, quote_df):
		# f, quote_df, p = self.convertToDataframe()

		# Remove some feature we wont be needing
		quote_df = quote_df.drop(columns=['record_type', 'location', 'risk_factor'])

		quote_df['wg_a_cal'] = quote_df.shopping_pt.astype('int64') * quote_df.A.astype('str')
		quote_df['wg_b_cal'] = quote_df.shopping_pt.astype('int64') * quote_df.B.astype('str')
		quote_df['wg_c_cal'] = quote_df.shopping_pt.astype('int64') * quote_df.C.astype('str')
		quote_df['wg_d_cal'] = quote_df.shopping_pt.astype('int64') * quote_df.D.astype('str')
		quote_df['wg_e_cal'] = quote_df.shopping_pt.astype('int64') * quote_df.E.astype('str')
		quote_df['wg_f_cal'] = quote_df.shopping_pt.astype('int64') * quote_df.F.astype('str')
		quote_df['wg_g_cal'] = quote_df.shopping_pt.astype('int64') * quote_df.G.astype('str')

		quote_df = quote_df.drop(columns=['A', 'B', 'C','D','E','F','G'])

		return quote_df

	def save_df_to_directory(self, df, filename):
		df.to_csv("dataset/" + filename , sep='\t', encoding='utf-8')
		print("dataframe successfully saved")


	def convertToDistinctRecord(self,df):
    
		col_names =  ['customer_ID', 'shopping_point', 'day', 'time', 'state','group_size','homeowner','car_age','car_value','c_age_oldest','age_youngest','married_couple','C_previous','duration_previous','W_A','W_B','W_C','W_D','W_E','W_F','W_G','cost']
		newDataframe  = pd.DataFrame(columns = col_names)

		print(len(df["customer_ID"].unique()))
		grouped = df.groupby('customer_ID')

		for name, group in tqdm(grouped):
			# print(name)

			customer_ID = group["customer_ID"].unique()
			shopping_pt = len(group["shopping_pt"])
			day = ""
			time = ""
			state = group["state"][0]
			group_size = group["group_size"][0]
			homeowner = group["homeowner"][0]
			car_age = group["car_age"][0]
			married_couple = group["married_couple"][0]
			C_previous = group["C_previous"][0]
			avg_cost = group["cost"].mean()
			group_size = group["group_size"][0]
			duration_previous = group["duration_previous"][0]

			wg_a_cal = group["wg_a_cal"].tolist()
			wg_b_cal = group["wg_b_cal"].tolist()
			wg_c_cal = group["wg_c_cal"].tolist()
			wg_d_cal = group["wg_d_cal"].tolist()
			wg_e_cal = group["wg_e_cal"].tolist()
			wg_f_cal = group["wg_f_cal"].tolist()
			wg_g_cal = group["wg_g_cal"].tolist()

			c_age_oldest = group["c_age_oldest"][0]
			c_age_youngest = group["c_age_youngest"][0]




			print(group["customer_ID"])


		

Data =  DataWrangler()
# Data.performOperation()

f, quote_df, p = Data.convertToDataframe()
df = Data.performWeightedAverage(quote_df)
df = Data.transformstateToCensusRegion(df)
df = Data.transformAgeToCategorical(df)
# Data.save_df_to_directory(df, "result.csv")
df = Data.convertToDistinctRecord(df)
