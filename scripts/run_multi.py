import subprocess

if __name__ == '__main__':
    if False:
        proc1 = subprocess.Popen(["target/debug/examples/fib"])
        proc2 = subprocess.Popen(["target/debug/examples/fib"])
        proc3 = subprocess.Popen(["target/debug/examples/fib"])
    else:
        proc1 = subprocess.Popen(["target/debug/examples/fib_simple"])
        proc2 = subprocess.Popen(["target/debug/examples/fib_simple"])
        proc3 = subprocess.Popen(["target/debug/examples/fib_simple"])
    proc1.wait()
    proc2.wait()
    proc3.wait()
