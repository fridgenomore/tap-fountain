# tap-fountain

This is a [Singer](https://singer.io) tap that produces JSON-formatted 
data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:
- Pulls raw data from Fountain's [Fountain API v2.0](https://developer.fountain.com/docs)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---