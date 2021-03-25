import os

import sys
if sys.version_info[0] > 2:
    from urllib.request import urlretrieve
else:
    from urllib import urlretrieve
import hashlib
import zipfile
import zlib
import shutil


def require_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)
    return None


def download(url, target_dir, filename=None):
    require_dir(target_dir)
    if filename is None:
        filename = url_filename(url)
    filepath = os.path.join(target_dir, filename)
    urlretrieve(url, filepath)
    return filepath

def check_zip_header(path):
    with open(path, 'rb') as MyZip:
        assert(MyZip.read(4)==b'PK\x03\x04')

def url_filename(url):
    return url.split('/')[-1].split('#')[0].split('?')[0]

def zip_files(files, outpath, inroot=False):
    with zipfile.ZipFile(outpath, 'w', zipfile.ZIP_DEFLATED) as myzip:
        for file in files:
            if inroot:
                myzip.write(file, arcname=file.name)
            else:
                myzip.write(file)

def md5(file_name):
    # Open,close, read file and calculate MD5 on its contents
    with open(file_name, 'rb') as f:
        hasher = hashlib.md5()  # Make empty hasher to update piecemeal
        while True:
            block = f.read(64 * (
                1 << 20))  # Read 64 MB at a time; big, but not memory busting
            if not block:  # Reached EOF
                break
            hasher.update(block)  # Update with new block
    return hasher.hexdigest()
   

def check_md5(file_name, orginal_md5):

    # Finally compare original MD5 with freshly calculated
    md5_returned = md5(file_name)
    if orginal_md5 == md5_returned:
        print('MD5 verified.')
        return True
    else:
        print('MD5 verification failed!')
        return False


def unzip(file, targetdir):
    with zipfile.ZipFile(file, "r", zipfile.ZIP_DEFLATED) as zip_ref:
        zip_ref.extractall(targetdir)