use data_types::learning::data_types;
use functions::learning::main_functions;
use control_flow::learning::control_flow;
use references_and_borrowing::learning::ref_and_borr;

mod data_types;
mod functions;
mod control_flow;
mod references_and_borrowing;


fn main() {
    data_types();
    main_functions();
    control_flow();
    ref_and_borr();
}
