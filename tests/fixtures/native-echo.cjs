// Child process: stdout is exclusively the native messaging wire protocol.
const { createNativeMessagingHost } = require('../../native-host/native-messaging');
const host = createNativeMessagingHost();
host.onMessage(message => host.send({ id: message.id, echo: message.text }));
