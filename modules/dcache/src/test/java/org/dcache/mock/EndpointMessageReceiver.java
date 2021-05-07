/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2021 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.mock;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.dcache.cells.CellStub;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static java.util.Objects.requireNonNull;
import static org.mockito.ArgumentMatchers.*;

/**
 * Build a generic set of responses describing how named cells will react
 * to stimuli.  Because we can only mock the send methods once, a single
 * builder instance must describe all responses for all named cells.
 */
public abstract class EndpointMessageReceiver<R extends CellMessageReceiver> {
  private final List<MessageResponse> messageResponses = new ArrayList<>();
  private final List<EndpointMessageReceiver.AdminCommandResponse> adminResponses = new ArrayList<>();
  private CellAddressCore currentAddress;
  private CellEndpoint endpoint;
  private CellStub stub;
  private R container;

  protected EndpointMessageReceiver(String address)
  {
    currentAddress = new CellAddressCore(address);
  }

  public EndpointMessageReceiver.MessageResponse thatOnReceiving(Class<? extends Message> messageType)
  {
    EndpointMessageReceiver.MessageResponse response = new EndpointMessageReceiver.MessageResponse(messageType);
    messageResponses.add(response);
    return response;
  }

  public EndpointMessageReceiver.AdminCommandResponse thatOnAdminCommand(String command)
  {
    EndpointMessageReceiver.AdminCommandResponse response = new EndpointMessageReceiver.AdminCommandResponse(command);
    adminResponses.add(response);
    return response;
  }

  public EndpointMessageReceiver andAnotherCell(String address)
  {
    currentAddress = new CellAddressCore(address);
    return this;
  }

  public EndpointMessageReceiver communicatingVia(CellStub stub) {
    this.stub = stub;
    return this;
  }

  public EndpointMessageReceiver communicatingVia(CellEndpoint endpoint) {
    this.endpoint = endpoint;
    return this;
  }

  public EndpointMessageReceiver respondingTo(R container) {
    this.container = container;
    return this;
  }

  public void accept(CellMessage envelope)
  {
    var destination = envelope.getDestinationPath().getDestinationAddress();

    if (destination.equals(currentAddress)) {
      messageResponses.forEach(r -> r.accept(envelope));
    }
  }

  public void build()
  {
    if (!adminResponses.isEmpty()) {
      mockAdminResponses();
    }

    if (!messageResponses.isEmpty()) {
      mockMessageResponses();
    }
  }

  protected abstract ResponseMessageDeliverable<R> aResponseTo(CellMessage message);

  private void mockAdminResponses()
  {
    Answer a = i -> {
      var target = i.getArgument(0, CellPath.class);
      var destination = target.getDestinationAddress();
      var message = i.getArgument(1, Serializable.class).toString();
      var response = adminResponses.stream()
          .map(r -> r.accept(destination, message))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .findFirst()
          .orElseThrow();
      return Futures.immediateFuture(response);
    };

    Mockito.doAnswer(a).when(stub).send(isA(CellPath.class), isA(Serializable.class), isA(Class.class));
  }

  private void mockMessageResponses()
  {
    Answer a = i -> {
      CellMessage envelope = i.getArgument(0, CellMessage.class);
      messageResponses.forEach(s -> s.accept(envelope));
      return null;
    };

    Mockito.doAnswer(a).when(endpoint).sendMessage(any());
  }

  /**
   * This class describes how a specific cell responds to a Message.
   */
  public class MessageResponse
  {
    private final CellAddressCore address = EndpointMessageReceiver.this.currentAddress;
    private final Class<? extends Message> reactsTo;
    private Class<? extends Message> responseType;
    private int code;
    private Serializable error;
    private Consumer<CellMessage> reaction = this::sendReply;

    public MessageResponse(Class<? extends Message> messageType)
    {
      reactsTo = requireNonNull(messageType);
      responseType = messageType;
    }

    public MessageResponse replies()
    {
      return this;
    }

    public EndpointMessageReceiver repliesWith(Class<? extends Message> responseType)
    {
      this.responseType = requireNonNull(responseType);
      return EndpointMessageReceiver.this;
    }

    public EndpointMessageReceiver repliesWithSuccess()
    {
      return repliesWithReturnCode(0);
    }

    public EndpointMessageReceiver repliesWithReturnCode(int code)
    {
      this.code = code;
      return EndpointMessageReceiver.this;
    }

    public MessageResponse repliesWithErrorObject(Serializable error)
    {
      this.error = error;
      return this;
    }

    public EndpointMessageReceiver andReturnCode(int code)
    {
      return repliesWithReturnCode(code);
    }

    public EndpointMessageReceiver storesRequestIn(SettableFuture<CellMessage> future)
    {
      requireNonNull(future);

      reaction = e -> {
        if (!future.set(e)) {
          throw new IllegalStateException("pool received multiple messages");
        }
      };

      return EndpointMessageReceiver.this;
    }

    private void sendReply(CellMessage envelope)
    {
      try {
        aResponseTo(envelope)
            .ofType(responseType)
            .withError(error)
            .withRc(code)
            .deliverTo(container);
      } catch (InterruptedException | IOException e) {
        throw new RuntimeException(e);
      }
    }

    public void accept(CellMessage envelope)
    {
      CellAddressCore destination = envelope.getDestinationPath().getDestinationAddress();

      if (destination.equals(address)) {
        Serializable messageObject = envelope.getMessageObject();

        if (reactsTo.isInstance(messageObject)) {
          reaction.accept(envelope);
        }
      }
    }
  }

  /**
   * This class describes how a cell reacts to an admin command.
   */
  public class AdminCommandResponse
  {
    private final CellAddressCore address = EndpointMessageReceiver.this.currentAddress;
    private final String command;
    private String reply;

    public AdminCommandResponse(String command)
    {
      this.command = requireNonNull(command);
    }

    public EndpointMessageReceiver repliesWith(String reply)
    {
      this.reply = requireNonNull(reply);
      return EndpointMessageReceiver.this;
    }

    public Optional<String> accept(CellAddressCore targetAddress, String command)
    {
      return address.equals(targetAddress) && this.command.equals(command)
          ? Optional.of(reply)
          : Optional.empty();
    }
  }
}
