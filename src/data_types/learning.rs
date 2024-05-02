pub fn data_types() {
    println!("****************************************************************");
    println!("Data Types");
    println!("****************************************************************");

    let data_f64: f64 = 2.0;
    let data_f32: f32 = 2.0;
    println!("data_f64 {}", data_f64);
    println!("data_f32 {}", data_f32);

    let second_data_f64: f64 = 2_000_000.0;
    let second_data_32: f32 = 2_000_000.0;
    println!("second_data_f64 {}", second_data_f64);
    println!("second_data_32 {}", second_data_32);

    println!("================================================================");
    println!("****************************************************************");
    println!("Basic Numeric Operations");
    println!("****************************************************************");

    let sum_int = 4 + 1;
    let sum_float = (400 + 1) as f64;
    let sum_float_2 = 400.0 + 1.555;
    let divide_int = 400 / 7;
    let divide_float = (400.0 / 7.0) as f64;
    let multiply_float: f64 = 1.122 * 2.0;

    println!("sum_int {}", sum_int);
    println!("sum_float {:.3}", sum_float);
    println!("sum_float_2 {:.3}", sum_float_2);
    println!("divide_int {}", divide_int);
    println!("divide_float {:.3}", divide_float);
    println!("multiply_float {:.3}", multiply_float);

    println!("================================================================");
    println!("****************************************************************");
    println!("Discriminant Implementation");
    println!("****************************************************************");

    let a = 5.0;
    let b = 15.0;
    let c = 2.0;
    println!("a {}", a);
    println!("b {}", b);
    println!("c {}", c);

    let d = b * b - 4.0 * a * c;
    println!("discriminant value {:.3}", d);

    println!("================================================================");
    println!("****************************************************************");
    println!("Array and Tuples");
    println!("****************************************************************");

    let tuple_1:(f32,i32,u8) = (1 as f32,2,4);

    println!("tuple_1 {:?}", tuple_1);

}
