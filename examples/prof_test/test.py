import pandas as pd

if __name__ == "__main__":

	df = pd.read_csv("execution_profile_rp.session.vivek-Lenovo-IdeaPad-Y480.vivek.016927.0002.csv", sep=",", header=0)
	df=df.drop(['iter','step'],axis=1)
	print df

	sec_df = pd.DataFrame(columns=["uid","iter","step"])
	sec_df.loc[0] = ['unit.000001','0','pre_loop']
	print sec_df

	res = pd.merge(df,sec_df,how='inner',on=['uid'])
	print res
