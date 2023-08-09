import zipfile 
import shutil 
import os 
import simpledbf 
import datetime 
import pandas as pd 

class CompileFiles: 

    def __init__(self, months_dir, target_dir, final_dir, compiled_filename): 
        self.months_dir = months_dir
        self.target_dir = target_dir
        self.final_dir = final_dir
        self.compiled_filename = compiled_filename
        
    def compile_zips(self):
        for month in os.listdir(self.months_dir):

            month_path = os.path.join(self.months_dir, month)
            for day_folder in os.listdir(month_path):
                day_path = os.path.join(month_path, day_folder)

                if os.listdir(day_path): day_zip = os.listdir(day_path)[0]
                else: continue 
                
                with zipfile.ZipFile(os.path.join(day_path, day_zip), 'r') as zip_ref: 
                    files = zip_ref.namelist()
                    for file in files: 
                        if file.startswith('shape/') and file.endswith('.dbf'):
                            target_file = file
                            break 

                    zip_ref.extract(target_file, self.target_dir)

        self.shape_path = os.path.join(self.target_dir, 'shape')

    def convert_dbfs(self):
        file_location = os.path.join(self.target_dir, 'shape')
        dfs = []
        for filename in os.listdir(file_location):
            filepath = os.path.join(file_location, filename)
            year, month, day = int(filename[18:22]), int(filename[23:25]), int(filename[26:28]) 
            
            dbf_ref = simpledbf.Dbf5(filepath)
            df = dbf_ref.to_dataframe()
            date = datetime.datetime(year, month, day)
            df['date'] = date 

            dfs.append(df)

        return pd.concat(dfs)
    
    def run(self):
        self.compile_zips()
        df = self.convert_dbfs()
        df.to_csv(os.path.join(self.final_dir, self.compiled_filename), index=False)
        shutil.rmtree(self.shape_path)

if __name__ == '__main__':
    month_dir = os.path.join('collection', 'Months') 
    target_dir = os.path.join('collection', 'DATA_FILES') 
    final_dir = os.path.join('Datasets')
    compiled_file_name = 'apr_to_may.csv'
    compiler = CompileFiles(month_dir, target_dir, final_dir, compiled_file_name)
    compiler.run()
    

            
                        
        

        

    