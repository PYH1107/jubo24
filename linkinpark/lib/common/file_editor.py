from zipfile import ZipFile
import os
import shutil
import csv
import subprocess


class FileEditor:
    
    def __init__(self):
        self.file_paths = []
    
    @staticmethod
    def create_file(path):
        with open(path, 'w', newline=''):
            pass  

    @staticmethod
    def create_dir(directory):
        os.mkdir(directory)

    @staticmethod
    def remove_file(path):
        os.remove(path)

    @staticmethod
    def remove_dir(path):
        shutil.rmtree(path)

    @staticmethod
    def copy_file(src_path, dst_path): 
        shutil.copy(src_path, dst_path)

    def _set_file_paths(self, directory):
        for root, directories, files in os.walk(directory):
            for filename in files:
                filepath = os.path.join(root, filename)
                self.file_paths.append(filepath)

    def _reset_file_paths(self):
        self.file_paths = []
    
    def create_zip(self, src_path, target_path, keep_parent_folder=True):
        self._reset_file_paths()
        self._set_file_paths(src_path)
        with ZipFile(target_path, 'w') as zf:
            for f in self.file_paths:
                if keep_parent_folder:
                    zf.write(f)
                else:
                    arcname = os.path.relpath(f, start=src_path)
                    zf.write(f, arcname=arcname)
            zf.close()

    def update_zip(self, src_path, target_path, keep_parent_folder=True):
        self._reset_file_paths()
        self._set_file_paths(src_path)
        with ZipFile(target_path, mode='a') as zf:
            for f in self.file_paths:
                if keep_parent_folder:
                    zf.write(f)
                else:
                    arcname = os.path.relpath(f, start=src_path)
                    zf.write(f, arcname=arcname)
            zf.close()
        self._handle_replaced_files(src_path, target_path)        
 
    def _handle_replaced_files(self, src_path, target_path):
        file_list = []
        for filepath in self.file_paths:
            file_list.append(src_path.split('/')[-1] + filepath.split(f"{src_path.split('/')[-1]}")[-1])

        print(file_list)
        check_zip = subprocess.Popen(f"zipinfo {target_path}", shell=True, stdout=subprocess.PIPE)
        zip_info = check_zip.stdout.read().decode("utf-8").split('\n')[2:-2]
        for filename in file_list:
            print(filename)
            create_time_list = []
            for f in zip_info:
                if f.split(' ')[-1] == filename:
                    create_time_list.append(f.split(' ')[-2])
                    if len(create_time_list) > 1:
                        os.system(f'zip -d {target_path} {filename}')

    @staticmethod
    def extract_zip(file_path, extract_dir):
        shutil.unpack_archive(file_path, extract_dir)
