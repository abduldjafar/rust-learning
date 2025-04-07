Of course! Here's a clean `README.md` you can use:

---

# Discriminant Calculator

A simple Rust CLI tool to calculate the **discriminant** \( D = b^2 - 4ac \) of a quadratic equation \( ax^2 + bx + c = 0 \).

Built with [`clap`](https://docs.rs/clap/latest/clap/) for easy command-line parsing.

## Formula

\[
D = b^2 - 4ac
\]

Where:
- `a` = coefficient of \( x^2 \) (quadratic term)
- `b` = coefficient of \( x \) (linear term)
- `c` = constant term

## Usage

First, build the project:

```bash
cargo build --release
```

Then, run it:

```bash
./target/release/discriminant_calculator -a <a> -b <b> -c <c>
```

### Example

```bash
./target/release/discriminant_calculator -a 1 -b -3 -c 2
```

Output:

```
Discriminant : 1
```

This means the equation \( x^2 - 3x + 2 = 0 \) has two distinct real roots.

## Help

You can also see the built-in help:

```bash
./target/release/discriminant_calculator --help
```

Example output:

```
Usage: discriminant_calculator [OPTIONS] --b <B> --a <A> --c <C>

Options:
  -b, --b <B>    the coefficient of x (the "linear" term)
  -a, --a <A>    the coefficient of x^2 (the "quadratic" term)
  -c, --c <C>    the constant term
  -h, --help     Print help
  -V, --version  Print version
```

## Requirements

- Rust (edition 2021 or newer)
- [`clap`](https://crates.io/crates/clap) crate