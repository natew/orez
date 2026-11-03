// where rooms are served on a host's origin.
export const ROOM_PATH = '/__orez/room/'
// where a room's ticket rides on its URL: a browser's WebSocket cannot carry
// headers, and a query parameter reaches every host the same way.
export const ROOM_TICKET_PARAM = 'ticket'
