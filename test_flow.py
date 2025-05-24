def simple_flow(flow_builder, scope):
    flow_builder.add_source(
        name="my_source",
        spec={
            "type": "constant",
            "value": {"name": "Shaurya", "score": 99}
        }
    )

# Test code merged here
if __name__ == "__main__":
    class MockFlowBuilder:
        def __init__(self):
            self.sources = []
        def add_source(self, name, spec):
            self.sources.append((name, spec))

    def test_simple_flow():
        flow_builder = MockFlowBuilder()
        scope = None
        simple_flow(flow_builder, scope)
        assert len(flow_builder.sources) == 1, "Expected one source to be added"
        name, spec = flow_builder.sources[0]
        assert name == "my_source", f"Expected source name 'my_source', got {name}"
        assert spec["type"] == "constant", f"Expected source type 'constant', got {spec['type']}"
        assert spec["value"] == {"name": "Shaurya", "score": 99}, f"Unexpected source value: {spec['value']}"
        print("Test passed: simple_flow adds the correct constant source")

    test_simple_flow()
