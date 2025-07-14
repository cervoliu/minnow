#include "reassembler.hh"

using namespace std;

void Reassembler::insert( uint64_t first_index, string data, bool is_last_substring )
{
  // Mark if this is the final substring
  if ( is_last_substring ) {
    final_index_known_ = true;
    final_index_ = first_index + data.length();
  }

  // Get writer for capacity checks
  auto& writer = output_.writer();

  // Calculate the range of indices we can accept (within capacity)
  uint64_t max_acceptable_index = next_assembled_index_ + writer.available_capacity();

  // Trim data that's beyond our capacity
  if ( first_index >= max_acceptable_index ) {
    // Entire segment is beyond capacity, discard it
    check_close_condition();
    return;
  }

  // Trim data that starts before our capacity window but extends into it
  if ( first_index + data.length() > max_acceptable_index ) {
    data = data.substr( 0, max_acceptable_index - first_index );
  }

  // Handle case where data overlaps with already assembled region
  if ( first_index < next_assembled_index_ ) {
    if ( first_index + data.length() <= next_assembled_index_ ) {
      // Entire segment is already assembled, discard it
      check_close_condition();
      return;
    }
    // Trim the already-assembled part
    uint64_t trim_amount = next_assembled_index_ - first_index;
    data = data.substr( trim_amount );
    first_index = next_assembled_index_;
  }

  // If we have any data left, store it
  if ( !data.empty() ) {
    // Merge with existing segments, handling overlaps
    merge_segment( first_index, data );
  }

  // Try to assemble consecutive data starting from next_assembled_index_
  assemble_consecutive_data();

  // Check if we can close the stream
  check_close_condition();
}

// How many bytes are stored in the Reassembler itself?
// This function is for testing only; don't add extra state to support it.
uint64_t Reassembler::count_bytes_pending() const
{
  uint64_t total = 0;
  for ( const auto& [index, data] : unassembled_data_ ) {
    total += data.length();
  }
  return total;
}

void Reassembler::merge_segment( uint64_t first_index, const std::string& data )
{
  uint64_t end_index = first_index + data.length();

  // Find all segments that overlap with our new segment
  auto it = unassembled_data_.lower_bound( first_index );

  // Check segments that start before our segment but might overlap
  if ( it != unassembled_data_.begin() ) {
    --it;
    if ( it->first + it->second.length() <= first_index ) {
      ++it; // No overlap, move to next
    }
  }

  // Merge overlapping segments
  std::string merged_data = data;
  uint64_t merged_start = first_index;
  uint64_t merged_end = end_index;

  while ( it != unassembled_data_.end() && it->first < end_index ) {
    uint64_t seg_start = it->first;
    uint64_t seg_end = it->first + it->second.length();

    if ( seg_start <= merged_end ) {
      // Merge this segment
      if ( seg_start < merged_start ) {
        // Extend backwards
        merged_data = it->second.substr( 0, merged_start - seg_start ) + merged_data;
        merged_start = seg_start;
      }
      if ( seg_end > merged_end ) {
        // Extend forwards
        merged_data += it->second.substr( merged_end - seg_start );
        merged_end = seg_end;
      }

      // Remove the old segment
      auto to_erase = it++;
      unassembled_data_.erase( to_erase );
    } else {
      ++it;
    }
  }

  // Store the merged segment
  unassembled_data_[merged_start] = merged_data;
}

void Reassembler::assemble_consecutive_data()
{
  auto& writer = output_.writer();

  // Keep assembling consecutive segments
  while ( true ) {
    auto it = unassembled_data_.find( next_assembled_index_ );
    if ( it == unassembled_data_.end() ) {
      break; // No segment starts at the expected index
    }

    // Found a segment that starts at the expected index
    std::string data = it->second;
    unassembled_data_.erase( it );

    // Push as much as we can to the writer
    uint64_t bytes_to_push = std::min( data.length(), writer.available_capacity() );
    if ( bytes_to_push > 0 ) {
      writer.push( data.substr( 0, bytes_to_push ) );
      next_assembled_index_ += bytes_to_push;
    }

    // If we couldn't push all data, put the remainder back
    if ( bytes_to_push < data.length() ) {
      unassembled_data_[next_assembled_index_] = data.substr( bytes_to_push );
      break; // No more capacity
    }
  }
}

void Reassembler::check_close_condition()
{
  if ( final_index_known_ && next_assembled_index_ >= final_index_ ) {
    output_.writer().close();
  }
}
