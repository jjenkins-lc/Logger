"""
CHAT GPT COMMENTS:
This is a log class that implements the basic functionality for writing logs to a file.

__init__ method initializes the required attributes for the class such as the file name, open file object, and others such as ELAPSED_TIME and line_counter.

__str__ method returns a human-readable description of the class object.

__exit__ method ensures that the file is closed when the program exits.

__del__ method ensures that the file is closed when the reference to the object is dropped.

write method writes the input text to the file along with the ELAPSED_TIME and the current time. It first checks if the input is a string, otherwise it raises a TypeError. It also checks if the input string is empty and raises a ValueError in such a case. Every 100,000th row it calls the _check_size method.

open method opens the file and sets the is_open attribute to True, in case the file is already open it raises an error.

close method closes the file and sets the is_open attribute to False, in case the file is already closed it raises an error.

_check_size method checks the size of the file after writing 100,000 rows and closes and re-opens the file if its size is larger than 50 MB.

Overall, this code provides a simple log class for writing logs to a file. It takes care of closing the file in case of unexpected terminations, and also re-opens the file if it becomes too large.

"""

import os
from datetime import datetime

#Log class that inherits from the object class
class Log(object):
    '''
    Creates a file object that supports a write method
    the write method will take in text and write it to a file along with a
    timestamp and a time since last write.

    supports a __exit__ and __del__ special method so that the file is
    closed if the object is garbage collected, or the program is terminated

    open method to open the file as needed, will not work if the file is already open
    close method to close the file as needed, will not work if the file is already closed

    the program will check every 100,000 lines written to see if its larger than 50MB,
    if it is, it will close and reopen the file.
    '''

    #variables to instantiate when object is constructed
    def __init__(self, file_name):
        self.file_name = file_name
        self.file = open(file_name, "a")
        self.is_open = True
        self.ts = None
        self.ELAPSED_TIME = 0
        self.line_counter = 0
        self.max_size = 50_000_000

    #if we use print on this object it will give us human readable description
    def __str__(self):
        return "Logger for file {}".format(self.file_name)

    #on program termination we make sure to close the file
    def __exit__(self):
        if self.file:
            print("program exit, closing file")
            self.file.close()

    #on object deletion or dropped reference we make sure to close its file
    def __del__(self):
        if self.file:
            print("closing file")
            self.file.close()
    
    #simple write method for this class we check and make sure the input is string
    #if its not then we raise an error for now 
    def write(self, text:str):
        try:
            if isinstance(text, str):
                ts = datetime.now()
                if not self.ts:
                    self.ts = ts
                else:
                    self.ELAPSED_TIME = ts - self.ts
                    self.ts=ts
                self.file.write(text + ", {}, {}\n".format(self.ELAPSED_TIME,ts))
                self.line_counter += 1
            else:
                raise TypeError("Input text must be a string")

            if len(text) == 0:
                raise ValueError("Input text is empty")
        except IOError as err:
            print("I/O error({}): {}".format(err.errno, err.strerror))

        except Exception as err:
            #raise error here and possibly write using own method
            pass

        if self.line_counter % 100_000 == 0:
            self._check_size()

    def open(self):
        if not self.is_open:
            try:
                self.is_open = True
                self.file = open(self.file_name, "a")
            except Exception as err:
                print("an error has occured with opening the file")
        else:
            raise Exception("file is already open")

    def close(self):
        if self.is_open:
            try:
                self.is_open = False
                self.file.close()
            except:
                print("an error occured with closing the file.")
        else:
            raise Exception("file is already closed")
    
    #check the size of the file if we have written 100,000 rows, if its larger
    #than 50MB then we close and open it again
    #this may not be the best way to do this
    def _check_size(self):
        current_size = os.path.getsize(self.file_name)
        if current_size > self.max_size:
            self.close()
            self.open()
