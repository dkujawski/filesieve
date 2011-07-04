'''
Created on 29/06/2011

@author: dave
'''
import ConfigParser
import hashlib
import os
import shutil

# where is the config file?
config_file = 'config/sieve.conf'
env_path = os.environ.get('FILESIEVE_ROOT')
if not env_path:
    # TODO: convert this into proper logging
    print "can't find the env var, assuming the config is up two dirs."
    rel_path = os.path.join('../../../')
    cur_path = os.path.abspath(__file__)
    env_path = os.path.abspath(os.path.join(cur_path, rel_path))

# get config settings    
config_path = os.path.join(env_path, config_file)
config = ConfigParser.SafeConfigParser()
config.read(config_path)

# set defaults from config
read_size = int(config.get('global', 'read_size', '1024'))
dup_dir = config.get('global', 'dup_dir', '/vol/musix/dups')

def walk(base_dir):
    '''
    recursively walk base_dir collecting md5 data from each file
    add data to a dict, when found dup md5 hashes move the matching file to the
    dup_dir as set in the config/sieve.conf
    '''
    bucket = dict()
    # walk the base_dir, we don't care about directory names.
    for root, _, files in os.walk(base_dir):
        for fn in files:
            fp = os.path.join(root, fn)
            double_read_size = (2 * read_size)
            if os.stat(fp).st_size > double_read_size:
                """ for files that are larger than twice the read_size only
                read the first and last chunks of the file.  this avoids having
                to load the entire file into memory.
                """
                first = ''
                last = ''                
                with open(fp) as fh:
                    first = fh.read(read_size)
                    neg_read_size = (-1 * read_size)
                    fh.seek(neg_read_size, os.SEEK_END)
                    last = fh.read(read_size)
                chunk = first + last
            else:
                """ for files that are smaller than twice the read_size go 
                ahead and generate the hash based on the entire file.
                """
                with open(fp) as fh:
                    chunk = fh.read()
            # build a hash key based on the file data
            key = get_hash_key(chunk)
            # check to see if we have seen this hash before
            if key in bucket:
                """ if we have seen this key before this file is a duplicate
                move it out of the way into the dup_dir. 
                """
                clean_dup(fp, dup_dir)
            else:
                bucket[key] = fp
    return bucket

def get_hash_key(data):
    """ generate a hash key using data
    """
    md5 = hashlib.md5()
    md5.update(data)
    key = md5.hexdigest()
    return key
    
def clean_dup(dup_file, dup_dir):
    """ move the dup_file over to a mirrored directory rooted in dup_dir 
    """
    dest = os.path.join(dup_dir, dup_file.lstrip('/'))
    dest_dir = os.path.dirname(dest)
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)
    shutil.move(dup_file, dest)
    return


if __name__ == "__main__":
    print "testing..."
    
    base_dir = "/vol/musix/Music"
    b = walk(base_dir)
    print "done!"