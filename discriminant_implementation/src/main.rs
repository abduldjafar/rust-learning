use clap::Parser;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(short, long, help="the coefficient of x (the \"linear\" term)")]
    b: f64,
    #[arg(short, long, help="the coefficient of x^2 (the \"quadratic\" term)")]
    a: f64,
    #[arg(short, long, help="the constant term ")]
    c: f64,

}

fn main() {
    let cli = Cli::parse();

    let d = (cli.b*cli.b) - (4 as f64 *cli.a*cli.c);

    println!("Discriminant : {}", d);
}
