#!/usr/bin/env python

import os
import sys
import xml.etree.ElementTree as ET

def main(property_name, property_file):
  value = ""
  if(os.path.isfile(property_file)):
    tree = ET.parse(property_file)
    root = tree.getroot()
    for child in root.findall('property'):
      name = child.find("name").text.strip()
      if name == property_name:
        value = child.find("value").text
        break
  print(value)
  sys.exit(0)

if __name__ == '__main__':
  if (len(sys.argv) == 3):
    property_name = sys.argv[1]
    property_file = sys.argv[2]
    main(property_name = property_name, property_file = property_file)
