const expect = require("expect.js");
const http = require("http");
const eio = require("engine.io");
const { Socket } = require("../");

describe("sendWithAck lifecycle", function () {
  this.timeout(5000);

  let httpServers = [];
  let engines = [];
  let ports = {};

  const start = (name, opts = {}) =>
    new Promise((resolve) => {
      const server = http.createServer();
      const engine = eio.attach(server, opts);
      httpServers.push(server);
      engines.push(engine);
      server.listen(0, () => {
        ports[name] = server.address().port;
        resolve(engine);
      });
    });

  before(async () => {
    await start("ack");
    await start("blackhole", { autoAck: false });
  });

  after(() => {
    httpServers.forEach((server) => server.close());
  });

  const connect = (name = "ack", opts = {}) =>
    new Socket(`http://localhost:${ports[name]}`, {
      transports: ["websocket"],
      ...opts,
    });

  it("completes only when the peer replies", (done) => {
    const socket = connect();
    socket.on("open", async () => {
      const response = await socket.sendWithAck("hello");
      expect(response).to.eql("hello");
      socket.close();
      done();
    });
  });

  it("plain send() does not wait for an ack", (done) => {
    const socket = connect("blackhole");
    socket.on("open", () => {
      expect(socket.send("no-ack")).to.be(socket);
      setTimeout(() => {
        expect(socket.readyState).to.eql("open");
        socket.close();
        done();
      }, 50);
    });
  });

  it("fails with a timeout error, and the late ack is dropped", (done) => {
    const socket = connect("blackhole");
    socket.on("open", () => {
      socket.sendWithAck("x", { timeout: 30 }, (err) => {
        expect(err).to.be.an(Error);
        expect(err.message).to.contain("timed out");
        expect(err.message).to.not.contain("disconnect");
        // emulate a late ack for id 0 arriving after the timeout
        const serverSocket = engines[1].clients[socket.id];
        serverSocket.send(JSON.stringify({ __eioAckReply: 0, data: "late" }));
        // the next request reuses id 0 and must not be completed by the late ack
        setTimeout(() => {
          const socket2 = connect();
          socket2.on("open", async () => {
            const res = await socket2.sendWithAck("y");
            expect(res).to.eql("y");
            socket.close();
            socket2.close();
            done();
          });
        }, 30);
      });
    });
  });

  it("ignores a second ack for the same id", (done) => {
    const socket = connect();
    let replies = 0;
    socket.on("open", () => {
      socket.sendWithAck("x", { timeout: 500 }, (err, res) => {
        if (!err) replies++;
      });
      setTimeout(() => {
        const serverSocket = engines[0].clients[socket.id];
        serverSocket.send(JSON.stringify({ __eioAckReply: 0, data: "one" }));
        serverSocket.send(JSON.stringify({ __eioAckReply: 0, data: "two" }));
        setTimeout(() => {
          expect(replies).to.eql(1);
          socket.close();
          done();
        }, 50);
      }, 20);
    });
  });

  it("fails pending acks with a disconnection error on close", (done) => {
    const socket = connect("blackhole");
    socket.on("open", () => {
      const connectionId = socket.id;
      socket.sendWithAck("x", { timeout: 10000 }, (err) => {
        expect(err).to.be.an(Error);
        expect(err.message).to.contain("disconnect");
        expect(err.message).to.contain(connectionId);
        expect(err.message).to.not.contain("timed out");
        done();
      });
      setTimeout(() => socket.close(), 20);
    });
  });

  it("does not consume an ack id when a middleware rejects", (done) => {
    const socket = connect();
    socket.use((data) => {
      if (data === "bad") throw new Error("nope");
    });
    socket.on("open", async () => {
      let rejected;
      try {
        await socket.sendWithAck("bad", { timeout: 200 });
      } catch (e) {
        rejected = e;
      }
      expect(rejected).to.be.an(Error);
      expect(rejected.message).to.contain("nope");
      const res = await socket.sendWithAck("good");
      expect(res).to.eql("good");
      // "good" must have reused id 0
      expect(socket._nextAckId).to.eql(1);
      socket.close();
      done();
    });
  });

  it("runs middlewares in order and can modify the payload", (done) => {
    const order = [];
    const socket = connect();
    socket
      .use((data) => {
        order.push(1);
        return data + "-1";
      })
      .use((data) => {
        order.push(2);
        return data + "-2";
      })
      .use(() => {
        order.push(3);
      });
    socket.on("open", async () => {
      const res = await socket.sendWithAck("msg");
      expect(order).to.eql([1, 2, 3]);
      expect(res).to.eql("msg-1-2");
      socket.close();
      done();
    });
  });

  it("skips later middlewares after a rejection", (done) => {
    let secondCalled = false;
    const socket = connect();
    socket
      .use(() => {
        throw new Error("first");
      })
      .use(() => {
        secondCalled = true;
      });
    socket.on("open", async () => {
      let err;
      try {
        await socket.sendWithAck("z");
      } catch (e) {
        err = e;
      }
      expect(err.message).to.contain("first");
      expect(secondCalled).to.eql(false);
      socket.close();
      done();
    });
  });

  it("keeps the connection open when a middleware throws", (done) => {
    const socket = connect();
    socket.use((data) => {
      if (data === "z") throw new Error("boom");
    });
    socket.on("open", async () => {
      let rejected;
      try {
        await socket.sendWithAck("z");
      } catch (e) {
        rejected = e;
      }
      expect(rejected).to.be.an(Error);
      expect(socket.readyState).to.eql("open");
      const res = await socket.sendWithAck("alive");
      expect(res).to.eql("alive");
      socket.close();
      done();
    });
  });

  it("resets ack ids on a new connection", (done) => {
    const socket = connect("blackhole");
    const blackhole = engines[1];
    let received = 0;
    blackhole.on("connection", (serverSocket) => {
      serverSocket.on("data", () => {
        received++;
      });
    });
    socket.on("open", () => {
      socket.sendWithAck("x", { timeout: 10000 }, () => {});
      socket.on("close", () => {
        const socket2 = connect("blackhole");
        socket2.on("open", () => {
          socket2.sendWithAck("y", { timeout: 10000 }, () => {});
          setTimeout(() => {
            // the new connection allocated id 0 again, it did not inherit the old connection's id 1
            expect(socket2._nextAckId).to.eql(1);
            expect(socket2._pendingAcks.has(0)).to.eql(true);
            socket2.close();
            done();
          }, 30);
        });
      });
      socket.close();
    });
  });

  it("per-message timeout does not change the default", (done) => {
    const socket = connect("blackhole", { ackTimeout: 1000 });
    socket.on("open", async () => {
      let err;
      try {
        await socket.sendWithAck("z", { timeout: 10 });
      } catch (e) {
        err = e;
      }
      expect(err).to.be.an(Error);
      expect(socket.opts.ackTimeout).to.eql(1000);
      socket.close();
      done();
    });
  });

  it("wraps ack ids back to the start, skipping ids still pending", (done) => {
    const socket = connect("blackhole");
    socket.on("open", () => {
      socket._nextAckId = 0x7ffffffe;
      socket.sendWithAck("a", { timeout: 10000 }, () => {});
      setTimeout(() => {
        expect(socket._pendingAcks.has(0x7ffffffe)).to.eql(true);
        socket.sendWithAck("b", { timeout: 10000 }, () => {});
        setTimeout(() => {
          // wrapped past MAX_ACK_ID, and skipped nothing (0 is free)
          expect(socket._pendingAcks.has(0)).to.eql(true);
          // occupy 0 too, then the next allocation must skip it
          socket.sendWithAck("c", { timeout: 10000 }, () => {});
          setTimeout(() => {
            expect(socket._pendingAcks.has(1)).to.eql(true);
            socket.close();
            done();
          }, 20);
        }, 20);
      }, 20);
    });
  });
});
