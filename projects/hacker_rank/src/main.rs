use std::process::Command;

fn main() {
    let output = Command::new("df")
        .arg("-h") // Human-readable format
        .output()
        .expect("Failed to execute df command");

    // Convert the output to a string and print
    let output_str = String::from_utf8_lossy(&output.stdout);
    println!("{}", output_str);
}
