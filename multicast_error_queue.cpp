#include "multicast_error_queue.h"

// Structure describing a file retransmission request
MulticastErrorQueue::FileRetransRequest::FileRetransRequest (
		const char *filename, const FileInfoHeader& finfo, uint32_t session_id,
		uint32_t number, uint32_t local_address)
{
	message_size = sizeof(MulticastMessageHeader) + sizeof(ReplyHeader) +
		sizeof(FileInfoHeader) + strlen(filename);
	message = malloc(message_size);
	MulticastMessageHeader *mih = new(message) MulticastMessageHeader(
		MULTICAST_FILE_RETRANS_REQUEST, session_id);
	mih->set_number(number);
	mih->set_responder(local_address);
	FileInfoHeader *fih = new(mih + 1) FileInfoHeader(finfo);
	memcpy(fih + 1, filename, strlen(filename));
	timestamp.tv_sec = 0;
	timestamp.tv_usec = 0;
	retrans_timeout = 0;
}

MulticastErrorQueue::TextError::TextError(uint8_t status,
		const char *error, uint32_t session_id, uint32_t number,
		uint32_t local_address)
{
	uint32_t error_length = strlen(error);
	message_size = sizeof(MulticastMessageHeader) + sizeof(ReplyHeader) +
		error_length;
	message = malloc(message_size);
	MulticastMessageHeader *mih = new(message) MulticastMessageHeader(
		MULTICAST_ERROR_MESSAGE, session_id);
	mih->set_number(number);
	mih->set_responder(local_address);
	ReplyHeader *rh = new(mih + 1) ReplyHeader(status, local_address,
		error_length);
	memcpy(rh + 1, error, error_length);
	timestamp.tv_sec = 0;
	timestamp.tv_usec = 0;
	retrans_timeout = 0;
}

// Add retransmission for the file 'fimename' to the queue
void MulticastErrorQueue::add_retrans_request(const char *filename,
		const FileInfoHeader& finfo, uint32_t session_id, uint32_t local_address)
{
	pthread_mutex_lock(&mutex);
	errors.push_front(new FileRetransRequest(filename, finfo,
		session_id, next_packet_number, local_address));
	next_packet_number++;
	n_errors++;
	pthread_mutex_unlock(&mutex);
}

// Move retransmission for the file 'fimename' in the end of the queue
std::list<MulticastErrorQueue::ErrorMessage*>::iterator
MulticastErrorQueue::get_error()
{
	pthread_mutex_lock(&mutex);
	std::list<ErrorMessage*>::iterator result =
		errors.begin();
	pthread_mutex_unlock(&mutex);
	return result;
}

// Move the error message pointed by 'arg' to the end of the queue
void MulticastErrorQueue::move_back(
		const std::list<ErrorMessage*>::iterator& arg)
{
	pthread_mutex_lock(&mutex);
	ErrorMessage *val = *arg;
	errors.erase(arg);
	errors.push_back(val);
	pthread_mutex_unlock(&mutex);
}

// Move the error message with the number 'number' from the queue
void MulticastErrorQueue::remove(uint32_t number)
{
	pthread_mutex_lock(&mutex);
	std::list<ErrorMessage*>::iterator i;
	for (i = errors.begin(); i != errors.end(); ++i) {
		if (((MulticastMessageHeader *)(*i)->message)->get_number() == number) {
			delete *i;
			errors.erase(i);
			n_errors--;
			break;
		}
	}
	pthread_mutex_unlock(&mutex);
}

