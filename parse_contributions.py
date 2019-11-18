import os
import sys
from cp_upload.pg import PG

class Main:

  def __init__(self):
    print("Construction Main")

  def run_main(self):
    pg = PG()
    
    # PB: This line will run all of the donations for supported files as defined in the pg.py
    # pg.upload_donations()

    # PB: This line will upload all of the voters from the voters directory. 
    # pg.upload_voters()

m = Main()
m.run_main()
