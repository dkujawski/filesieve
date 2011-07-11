'''
Created on 29/06/2011

@author: dave
'''
from collections import defaultdict
import ConfigParser
import hashlib
import os
import shutil

class Sieve(object):
    """ Container object to init global config data and identify dup files.
    The main sieve.conf file can be in one of two places.  First the env is
    checked for the FILESIEVE_ROOT var.  If this is not found it is assumed
    that the config file is in the config dir three dirs up from this module. 
    
    Example ::
    
        base_dir = "/vol/musix/Music"
        s = Sieve()
        file_dict = s.walk(base_dir)
                
    """
    def __init__(self):        
        # set defaults from config
        config = self.__get_config()
        self.read_size = int(config.get('global', 'read_size', '1024'))
        self.dup_dir = config.get('global', 'dup_dir', '/tmp/sieve/dups')
        # dup trackers
        self.dup_keys = set()
        # main data bucket
        self.data = defaultdict(list)

    def __get_config(self):
        """ dig out the config file
        """
        # where is the config file?
        config_file = 'config/sieve.conf'
        env_path = os.environ.get('FILESIEVE_ROOT')
        if not env_path:
            # TODO: convert this into proper logging
            rel_path = os.path.join('../../../')
            cur_path = os.path.abspath(__file__)
            env_path = os.path.abspath(os.path.join(cur_path, rel_path))
        # get config settings    
        config_path = os.path.join(env_path, config_file)
        if not os.path.exists(config_path):
            print "unable to locate config file, using defaults."
        config = ConfigParser.SafeConfigParser()
        config.read(config_path)
        return config
    
    @property
    def dup_count(self):
        """ return number of dups found since object init
        """
        return len(self.dup_keys)
    
    def walk(self, base_dir):
        """ recursively walk base_dir collecting md5 data from each file
        add data to a dict, when found dup md5 hashes move the matching file to
        the dup_dir as set in the config/sieve.conf
        """
        assert isinstance(base_dir, str), 'base_dir must be a string type'
        if not os.path.exists(base_dir):
            print "base directory tree does not exist:\n\t%s" % (base_dir,)
            return dict(self.data)
        # walk the base_dir, we don't care about directory names.
        for root, _, files in os.walk(base_dir):
            for fn in files:
                # build the full pile path
                fp = os.path.join(root, fn)
                # process the file data to get the hash key
                key = process_file(fp, self.read_size)
                # check to see if we have seen this hash before
                if key in self.data:
                    """ if we have seen this key before this file is a dup.
                    add the key to the dup set
                    """
                    self.dup_keys.add(key)
                # add the key and the data dict
                self.data[key].append(fp)
        return dict(self.data)

def process_file(file_path, read_size):
    """ generate a hash key based on the file_path.  if the file is larger than
    twice the read_size base the key on the first and last data chunks of the 
    file.  otherwise use the entire file data.
    """
    double_read_size = (2 * read_size)
    if os.stat(file_path).st_size > double_read_size:
        """ for files that are larger than twice the read_size only
        read the first and last chunks of the file.  this avoids
        having to load the entire file into memory.
        """
        first = ''
        last = ''                
        with open(file_path) as fh:
            first = fh.read(read_size)
            neg_read_size = (-1 * read_size)
            fh.seek(neg_read_size, os.SEEK_END)
            last = fh.read(read_size)
        chunk = first + last
    else:
        """ for files that are smaller than twice the read_size go 
        ahead and generate the hash based on the entire file.
        """
        with open(file_path) as fh:
            chunk = fh.read()
    # build a hash key based on the file data
    return get_hash_key(chunk)

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
