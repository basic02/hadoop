#!/usr/bin/env python

import os
import sys
import xml.etree.ElementTree as ET

def main(config_file, config_name, config_value):
  try:
    tree = ET.parse(config_file)
  except ExpatError:
    print("[E] Error while parsing file " + str(config_file))
    sys.exit(1)
  root = tree.getroot()
  found = False
  for child in root.findall('property'):
    name = child.find("name").text.strip()
    if name == config_name:
      child.find("value").text = config_value
      found = True
      break

  if not found:
    new_property = ET.SubElement(root, 'property')
    property_name = ET.SubElement(new_property, 'name')
    property_name.text = config_name
    property_value = ET.SubElement(new_property, 'value')
    property_value.text = config_value

  tree.write(config_file)
  sys.exit(0)

if __name__ == '__main__':
  if (len(sys.argv) == 4):
    config_name = sys.argv[1]
    config_value = sys.argv[2]
    config_file = sys.argv[3]

    if not (os.path.isfile(config_file)):
      print("[E] Config file " + str(config_file) + " does not exit to perform update action on " + config_name + " config.")
      sys.exit(1)

    main(config_file = config_file, config_name = config_name, config_value = config_value)
