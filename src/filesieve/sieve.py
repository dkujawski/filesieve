'''
Created on 29/06/2011

@author: dave
'''
import hashlib
import os
import shutil

def walk(base_dir):
    '''
    recursively walk base_dir collecting md5 data from each file
    '''
    bucket = dict()
    duplicates = dict()
    read_size = 1024
    dup_dir = "/vol/musix/dups"
    for root, _, files in os.walk(base_dir):
        for fn in files:
            fp = os.path.join(root, fn)
            first = ''
            last = ''
            with open(fp) as fh:
                if os.stat(fp).st_size > read_size:
                    first = fh.read(read_size)
                    neg_read_size = -1 * read_size
                    fh.seek(neg_read_size, os.SEEK_END)
                    last = fh.read(read_size)
                    chunk = first + last
                else:
                    chunk = fh.read(read_size)
            md5 = hashlib.md5()
            md5.update(chunk)
            key = md5.hexdigest()
            if key in bucket:
                duplicates[key] = fp
                clean_dup(fp, dup_dir)
                #print key, fp, bucket.get(key)
            else:
                bucket[key] = fp
    return bucket, duplicates

def clean_dup(path, dup_dir):
    dest = os.path.join(dup_dir, path.lstrip('/'))
    dest_dir = os.path.dirname(dest)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    shutil.move(path, dest)
    return


if __name__ == "__main__":
    print "testing..."
    
    base_dir = "/vol/musix/Music"
    b, d = walk(base_dir)
    print d
    print "done!"