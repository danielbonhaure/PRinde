# Schema Folder

## Alias File

The `alias.json` file contains a JSON object that is used to perform keys replacements inside forecast specification files.
 This is used to achieve a more clear and customized specification and avoid writting keys with the pSIMS format.
   
## Simulation Schema File

This JSON Schema Document defines a set of *structural* rules that every Simulation instance should conform with (*).
It's main goal is to validate simulations in order to detect invalid input and avoid running those that will eventually raise errors.
See http://json-schema.org for information on the JSON Schema definition.

*Note*: this checks are performed after replacing simulation keys found in the `alias.json` file.

(*) Note that other checks are performed on runtime, ex. the amount of soil horizons is checked once the corresponding soil file is loaded.