package com.namelessmc.plugin.common;

import com.namelessmc.java_api.NamelessAPI;
import com.namelessmc.java_api.NamelessUser;
import com.namelessmc.java_api.exception.NamelessException;
import com.namelessmc.plugin.common.command.AbstractScheduledTask;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class UserCache implements Reloadable {

	private final NamelessPlugin plugin;

	private @Nullable AbstractScheduledTask task;
	private List<String> usernames = Collections.emptyList();
	private List<String> minecraftUsernames = Collections.emptyList();
	private List<UUID> minecraftUuids = Collections.emptyList();

	UserCache(final NamelessPlugin plugin) {
		this.plugin = plugin;
	}

	@Override
	public void unload() {
		if (this.task != null) {
			this.task.cancel();
			this.task = null;
		}
		this.usernames = Collections.emptyList();
		this.minecraftUsernames = Collections.emptyList();
		this.minecraftUuids = Collections.emptyList();
	}

	@Override
	public void load() {
		this.refreshNow();
		task = this.plugin.scheduler().runTimer(this::refreshNow, Duration.ofMinutes(5));
	}

	public CompletableFuture<Void> refreshNow() {
		final CompletableFuture<Void> future = new CompletableFuture<>();
		this.plugin.scheduler().runAsync(() -> {
			this.plugin.logger().fine("Refreshing user cache");
			final NamelessAPI api = this.plugin.apiProvider().api();
			if (api == null) {
				future.complete(null);
				return;
			}

			try {
				final List<NamelessUser> users = api.users().makeRequest();

				final List<String> usernames = new ArrayList<>(users.size());
				final List<String> minecraftUsernames = new ArrayList<>(users.size());
				final List<UUID> minecraftUuids = new ArrayList<>(users.size());

				for (NamelessUser user : users) {
					usernames.add(user.username());
					if (user.minecraftUsername() != null) {
						minecraftUsernames.add(user.minecraftUsername());
					}
					final UUID minecraftUuid = user.minecraftUuid();
					if (minecraftUuid != null) {
						minecraftUuids.add(minecraftUuid);
					}
				}

				this.usernames = usernames;
				this.minecraftUsernames = minecraftUsernames;
				this.minecraftUuids = minecraftUuids;
				future.complete(null);
			} catch (NamelessException e) {
				this.plugin.logger().logException(e);
				future.completeExceptionally(e);
			}
		});
		return future;
	}

	private List<String> search(Collection<String> original, @Nullable String part) {
		if (part == null) {
			return Collections.emptyList();
		}
		return original.stream().filter(s -> s.startsWith(part)).collect(Collectors.toUnmodifiableList());
	}

	public List<String> usernames() {
		return this.usernames;
	}

	public List<String> usernamesSearch(@Nullable String part) {
		return this.search(this.usernames, part);
	}

	public List<String> minecraftUsernames() {
		return this.minecraftUsernames;
	}

	public List<UUID> minecraftUuids() {
		return this.minecraftUuids;
	}

	public List<String> minecraftUsernamesSearch(@Nullable String part) {
		return this.search(this.minecraftUsernames, part);
	}

}
