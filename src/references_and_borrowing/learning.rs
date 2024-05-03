pub fn ref_and_borr(){
    println!("================================================================");
    println!("****************************************************************");
    println!("Rust References and Borrowing");
    println!("****************************************************************");

    let s1 = String::from("hello world");
    let len = calculate_lenght_with_ref_params(&s1);

    println!("calculate_lenght_with_ref_params {}", len);

    let mut  s2 = String::from("hello world");
    change(&mut s2);

    println!("after change s2 {}", s2);

    let mut s3 = String::from("hello world");

    // a mutable variable just can refer once at the time. if want to do multiple should have another process to move the referebce
    let r1 = &mut s3;
    println!("{}", r1);
    let r2 = &mut s3;
    println!("{}", r2)



}

fn calculate_lenght_with_ref_params(s: &str) -> usize {
    let d = s.len();
    let x = d;
    println!("{}", x);

    d
}

fn change(s: &mut String){
    s.push_str("aaa");
}