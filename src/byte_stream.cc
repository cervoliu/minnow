#include "byte_stream.hh"
#include <string_view>

using namespace std;

ByteStream::ByteStream( uint64_t capacity ) : capacity_( capacity ) {}

void Writer::push( string_view data )
{
  if ( available_capacity() ) {
    if ( data.size() > available_capacity() ) {
      data.remove_suffix( data.size() - available_capacity() );
    }
    buffer_ += data;
    bytes_pushed_ += data.size();
  }
}

void Writer::close()
{
  closed_ = true;
}

bool Writer::is_closed() const
{
  return closed_;
}

uint64_t Writer::available_capacity() const
{
  return capacity_ - buffer_.size();
}

uint64_t Writer::bytes_pushed() const
{
  return bytes_pushed_;
}

string_view Reader::peek() const
{
  return buffer_;
}

void Reader::pop( uint64_t len )
{
  if ( len > buffer_.size() ) {
    set_error();
    return;
  }
  buffer_.erase( 0, len );
  bytes_popped_ += len;
}

bool Reader::is_finished() const
{
  return closed_ && buffer_.empty();
}

uint64_t Reader::bytes_buffered() const
{
  return buffer_.size();
}

uint64_t Reader::bytes_popped() const
{
  return bytes_popped_;
}

