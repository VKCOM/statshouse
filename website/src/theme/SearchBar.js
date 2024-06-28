import React from 'react';
import EnhancedChat from 'enhancedocs-chat';

import 'enhancedocs-chat/dist/style.css';

export default function SearchBarWrapper(props) {
  return (
    <EnhancedChat
      config={{
        apiBaseURL: "your_api_base_url",
        accessToken: "your_app_access_token",
      }}
      {...props}
    />
  );
}
