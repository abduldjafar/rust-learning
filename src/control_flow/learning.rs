pub fn control_flow(){
    println!("================================================================");
    println!("****************************************************************");
    println!("Rust functions");
    println!("****************************************************************");

    let condition = true;

    let value_from_cotrol_flow = if condition { 1 } else { 0 };
    println!("value_from_cotrol_flow {}", value_from_cotrol_flow);

    let mut index = 0;

    let loop_control = loop{
            index += 1;

            if index == 10{
                break index;
            }
    };

    println!("loop_control_value {}", loop_control);
}