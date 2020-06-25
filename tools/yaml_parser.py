import yaml

class yamlParser:
  def __init__(self, filename):
    self.filename=filename

  def read(self):
    with open(self.filename, 'r') as stream:
        try:
            creds = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return creds

  ## Incase you want to load other yaml files
  # def set_filename(self, filename):
  #  self.filename = filename

## Use case
#parser = yamlParser("/credentials/aws.yaml")
#data = parser.read()

# did stuff with data
#parser.set_filename("credentials/psql.yaml")
#data = parser.read() 
####
