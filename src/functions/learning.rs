pub fn main_functions() {
    println!("================================================================");
    println!("****************************************************************");
    println!("Rust functions");
    println!("****************************************************************");
    function_without_param();
    let x = function_with_params(2 as f32, 3 as f64);
    println!("x: {}", x);

    let use_expression = {
        let x = function_with_params(2 as f32, 3 as f64);
        x
    };

    println!("use_expression: {:.3}", use_expression);

    let fn_plus_one = plus_one(1);
    println!("fn_plus_one: {:.3}",fn_plus_one)
}

fn function_without_param(){
    println!("hello from fn without param")
}

fn function_with_params(val1: f32, val2: f64) -> f32 {
    val1 * val2 as f32
}

fn plus_one(x: i32) -> i32 {
    x + 1
}