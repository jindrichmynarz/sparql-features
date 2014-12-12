# sparql-features

A command-line tool to load feature frequencies from a SPARQL endpoint. Features are triples consisting of property URI, resource URI, and directionality. The feature selection is based on a list of classes, a list of outbound properties, and a list of inbound properties. Resource URIs in features are restricted to instances of selected classes. Only the properties from preconfigured lists of outbound and inbound properties are taken into account. Outbound property is a property in a triple pattern `?resource ?outboundProperty [] .`, where `?resource` is bound to one of the instances of the preconfigured classes. Conversely, inbound property is a property in a triple pattern `[] ?inboundProperty ?resource`.

For each feature, the tool retrieves the number of its occurrences. For example, if we have a feature with the `<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>` property and `<http://schema.org/Organization>` object, then the frequency would be computed as the number of distinct bindings of `?resource` in the triple pattern `?resource <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Organization> .`. Conversely, if we have a feature with the `<http://www.w3.org/2004/02/skos/core#related>` property and `<http://example.com/concept/1234>`, then the frequency would be computed for the triple pattern `<http://example.com/concept/1234> <http://www.w3.org/2004/02/skos/core#related> ?resource .`. 

Feature extraction (`--task features`) produces a TSV file with features per single resource into the specified directory. These files are named with SHA1 hashes of resources' URIs. The output of feature statistics (`--task stats`) is serialized into a TSV file, where each line contains a feature and its frequency in the dataset in question.

## Usage

The tool has a command-line interface and configuration in [YAML](http://www.yaml.org/). The feature selection is defined in the configuration file. The default configuration file can be found in [`resource/config.yaml`](https://github.com/jindrichmynarz/sparql-features/blob/master/resources/config.yaml). Start by copying this file to create your private configuration file. In the configuration, you need to describe the SPARQL endpoint you will be using. For example, for default Virtuoso installation on `localhost` use:

```yaml
sparql-endpoint:
  query-url: http://localhost:8890/sparql
  update-url: http://localhost:8890/sparql-auth
  username: dba
  password: dba
```

The second part of the configuration in the `features` section is the definition of the features to be taken into account. Fill in the URI of the named graph to consider using the `source-graph` attribute. The `page-size` attribute is used to limit the number of bindings processed in 1 request to a SPARQL endpoint. If you receive time-out errors when running the tool, consider lowering the `page-size`. Via the `parallel-execution` boolean flag you can turn on parallel execution of SPARQL queries. If there's uneven load on the SPARQL endpoint and it tends to time-out queries sometimes, you can set the `retry-count` attribute to specify how many times should queries be retried. Using the `classes` attribute provide a list of absolute URIs of classes, the instances of which should be considered. In the `properties` section, list absolute URIs of outbound and inbound properties to be considered for the class instances.

If you don't provide a value for some attribute, its default value from [`resources/config.yaml`](https://github.com/jindrichmynarz/sparql-features/blob/master/resources/config.yaml) will be used.

You can run the tool using the [pre-compiled JAR file](https://github.com/jindrichmynarz/sparql-features/releases/tag/v0.2), to which you pass the configuration as a parameter. To get help about the command-line parameters of the tool, run the following:

```bash
java -jar sparql-features.jar -h
```

## Tips

* For feature statistics computation on large datasets, it may be better to use multiple configuration files with different settings (page size, parallel execution) for different class-property combinations. The results can be easily concatenated, for example by using:

```bash
{ cat stats_1.tsv; tail -n+2 stats_2.tsv; } > stats.tsv
```

* Usually, it's better to set the page size to be less than 10000, because that's the default maximum response size of Virtuoso endpoints. This way you can avoid trimmed results. 

## Compilation

The tool can be compiled using [Leiningen](http://leiningen.org/):

```bash
git clone https://github.com/jindrichmynarz/sparql-features.git
cd sparql-features
lein clean; lein uberjar
```

## Caveats

* The tool currently works only with Virtuoso due to the reliance on Virtuoso-specific stopping condition for paged SPARQL Update operations. The SPARQL 1.1 Update specification doesn't prescribe what should be in a response to an Update operation that doesn't affect any triples, so different implementations provide different responses. More details about this issue can be found [here](http://answers.semanticweb.com/questions/29420/stopping-condition-for-paged-sparql-update-operations/29422). An alternative solution is to provide an explicit count of triples that would be affected by an Update operation without paging via `LIMIT` and `OFFSET` and then stop iterating when this number is reached.

## License

Copyright © 2014 Jindřich Mynarz

Distributed under the Eclipse Public License version 1.0. 
