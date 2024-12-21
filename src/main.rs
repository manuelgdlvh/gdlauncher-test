use std::cmp::max;
use std::env;
use std::fs::File;
use std::str::FromStr;
use std::thread::available_parallelism;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use memmap::{Mmap, MmapOptions};
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;

const MIN_PARALLELISM: usize = 4;
const ITEM_RANGE_SIZE: usize = 100;
const RELATIVE_FILE_PATH: &str = "/resources/challenge_input.txt";
const NUMBERS_BUFFER_SIZE: usize = ITEM_RANGE_SIZE + 1;
const MARGIN_AVOID_LINE_BREAK: usize = 2;
const SPLIT_MARKER: u8 = b'\n';
const STR_U128_LEN: usize = 39;


fn main() -> anyhow::Result<()> {
    let parallelism = max(MIN_PARALLELISM, available_parallelism()?.get());
    let current_dir = env::current_dir()?;
    let current_dir_str = current_dir.to_str().context("Path to str conversion failed")?;
    let file_path = format!("{}{}", current_dir_str, RELATIVE_FILE_PATH);

    let file = File::open(file_path)?;
    let mmap = unsafe { MmapOptions::new().map(&file) }?;


    let start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards").as_millis();

    let result: Vec<u128> = get_bounds(&mmap, parallelism)
        .par_iter()
        .flat_map(|(left, right)| process(&mmap, *left, *right))
        .collect();

    println!("{} ms", SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards").as_millis() - start);

    println!("{} invalid numbers found.\n{:?}", result.len(), result);
    Ok(())
}


fn get_bounds(mmap: &Mmap, parallelism: usize) -> Vec<(usize, usize)> {
    let bytes_per_chunk = mmap.len() / parallelism;
    let mut bounds: Vec<(usize, usize)> = Vec::with_capacity(parallelism);

    let mut previous_left_bound = 0;
    for num_core in 1..=parallelism {
        let left_bound = previous_left_bound;
        let (right_bound, right_bound_overflow) = get_right_bounds(&mmap, num_core * bytes_per_chunk);
        previous_left_bound = right_bound + MARGIN_AVOID_LINE_BREAK;
        bounds.push((left_bound, right_bound_overflow))
    }

    bounds
}

// Calculate the next valid index within the bounds, considering an overflow of ITEM_RANGE_SIZE (100) items.
// This ensures the first ITEM_RANGE_SIZE items of each segment are processed.
fn get_right_bounds(mmap: &Mmap, ini_pos: usize) -> (usize, usize) {
    let file_len = mmap.len();
    let mut idx = ini_pos;

    let mut right_bound = file_len - MARGIN_AVOID_LINE_BREAK;
    let mut right_bound_overflow = right_bound;

    let mut overflow_count = 0;
    while idx < file_len && overflow_count < ITEM_RANGE_SIZE {
        if mmap[idx] == SPLIT_MARKER {
            overflow_count += 1;
            match overflow_count {
                1 => { right_bound = idx - 1; }
                ITEM_RANGE_SIZE => { right_bound_overflow = idx - 1 }
                _ => {}
            }
        }
        idx += 1;
    }

    (right_bound, right_bound_overflow)
}


fn process(mmap: &Mmap, left_bound: usize, right_bound: usize) -> Vec<u128> {

    // Max length of u128 represented as str
    let mut str_buffer: [u8; STR_U128_LEN] = [0; STR_U128_LEN];
    let mut str_buffer_idx = STR_U128_LEN;

    // Used fixed array instead of VecDeque because read operations are more intensive than insertions.
    let mut numbers: [u128; NUMBERS_BUFFER_SIZE] = [0; NUMBERS_BUFFER_SIZE];
    let mut numbers_idx = 0;

    let mut result = Vec::new();
    for &byte in mmap[left_bound..=right_bound].iter().rev() {
        if byte != SPLIT_MARKER {
            str_buffer_idx -= 1;
            str_buffer[str_buffer_idx] = byte;
            continue;
        }

        let new_number = parse_number_from_str_buffer(&str_buffer[str_buffer_idx..STR_U128_LEN]);
        if numbers_idx == NUMBERS_BUFFER_SIZE {
            process_next_number(&mut result, &mut numbers, new_number);
        } else {
            numbers[numbers_idx] = new_number;
            numbers_idx += 1;
        }

        // Reset
        str_buffer[str_buffer_idx..STR_U128_LEN].fill(0);
        str_buffer_idx = STR_U128_LEN;
    }

    let new_number = parse_number_from_str_buffer(&str_buffer[str_buffer_idx..STR_U128_LEN]);
    process_next_number(&mut result, &mut numbers, new_number);

    if !is_number_valid(numbers[0], &numbers[1..=ITEM_RANGE_SIZE]) {
        result.push(numbers[0]);
    }

    result
}


fn process_next_number(result: &mut Vec<u128>, numbers: &mut [u128; NUMBERS_BUFFER_SIZE], new_number: u128) {
    if !is_number_valid(numbers[0], &numbers[1..=ITEM_RANGE_SIZE]) {
        result.push(numbers[0]);
    }

    numbers.rotate_left(1);
    numbers[ITEM_RANGE_SIZE] = new_number;
}

fn parse_number_from_str_buffer(str_buffer: &[u8]) -> u128 {
    let number_str = std::str::from_utf8(str_buffer).expect("Parse byte buffer to str successfully");
    u128::from_str(number_str).expect("Parse number from str successfully")
}


// Skip all numbers greater than the target (excluding the target itself). The target and 0 may still be valid candidates together.
fn is_number_valid(target: u128, candidates: &[u128]) -> bool {
    candidates.iter()
        .enumerate()
        .filter(|(idx, &outer_ref)| outer_ref <= target)
        .any(|(idx, &outer_ref)| {
            candidates.iter()
                .skip(idx + 1)
                .any(|&inner_ref| inner_ref <= target && inner_ref + outer_ref == target)
        })
}
