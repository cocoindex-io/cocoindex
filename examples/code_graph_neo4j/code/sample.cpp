namespace demo {
struct Base { int value; };
class Worker : public Base {
public:
    int run(int x) { return external(x) + helper(x); }
    int helper(int x) { return x + value; }
};
}
